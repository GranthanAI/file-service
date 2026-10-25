"""YouTube source lifecycle: attach (idempotent), extract (leased, retried, bounded), store canonical transcript, remove.

Flow
  attach()          validate URL -> one live source per (owner, video) -> create `files` row (UPLOADING) + `youtube_sources`
                    (PENDING); concurrent/repeated submissions return the same document.
  run_extraction()  claim (compare-and-set lease) -> discover tracks -> select -> fetch -> validate -> canonical JSON to object
                    storage -> files UPLOADED + `file.uploaded` outbox row (the EXISTING ingestion path takes over and sets
                    READY once chunks are indexed) -> source FETCHED.
A source is "ready for context" only when extraction is FETCHED **and** files.status is READY (indexed).
"""

import asyncio
import hashlib
import io
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

from minio import Minio
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.enums import FileStatus
from app.core.exceptions import InvalidStateTransitionError
from app.core.logging import get_logger
from app.models.file import File
from app.models.youtube_source import YoutubeSource
from app.repositories.file_repository import FileRepository
from app.repositories.file_version_repository import FileVersionRepository
from app.repositories.outbox_repository import OutboxRepository
from app.services.upload_service import _build_file_uploaded_payload
from app.storage.key_builder import build_storage_key
from app.utils.id_generator import new_uuid7
from app.youtube import transcript as tr
from app.youtube.provider import SAFE_MESSAGES, TranscriptError, TranscriptProvider, TransientDependency
from app.youtube.urls import ParsedVideo, parse_youtube_url

logger = get_logger(__name__)

_semaphore: asyncio.Semaphore | None = None


def _sem() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(max(1, settings.YOUTUBE_MAX_CONCURRENT_FETCHES))
    return _semaphore


class QuotaExceeded(Exception):
    def __init__(self, retry_after_seconds: int = 3600) -> None:
        super().__init__("youtube source quota exceeded")
        self.retry_after_seconds = retry_after_seconds


@dataclass
class AttachResult:
    source: YoutubeSource
    created: bool
    start_extraction: bool


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _placeholder_name(video_id: str) -> str:
    return f"YouTube {video_id} transcript.json"


def display_filename(title: str | None, video_id: str) -> str:
    if not title:
        return _placeholder_name(video_id)
    clean = "".join(" " if (ord(c) < 0x20 or c in '/\\<>:"|?*') else c for c in title)
    clean = " ".join(clean.split())[:150].strip(" .")
    return f"{clean} (YouTube transcript).json" if clean else _placeholder_name(video_id)


def needs_extraction(source: YoutubeSource, now: datetime | None = None) -> bool:
    """Should a (re)submission start a worker? Pending work, a worker whose lease expired, or a retry that is due."""
    now = now or _now()
    if source.extraction_status == "PENDING":
        return True
    if source.extraction_status == "FETCHING":
        return source.lease_until is None or source.lease_until <= now
    if source.extraction_status == "FAILED_RETRYABLE":
        return source.next_retry_at is None or source.next_retry_at <= now
    return False


async def _live_source(db: AsyncSession, owner_id: UUID, video_id: str, *, lock: bool) -> YoutubeSource | None:
    q = select(YoutubeSource).where(
        YoutubeSource.owner_id == owner_id, YoutubeSource.video_id == video_id, YoutubeSource.removed_at.is_(None)
    )
    if lock:
        q = q.with_for_update()
    return (await db.execute(q)).scalar_one_or_none()


async def _retire_source(db: AsyncSession, source: YoutubeSource, file: File | None) -> None:
    """Frees the (owner, video) slot. Only used when no indexed content exists for the old row (nothing to un-index)."""
    source.removed_at = _now()
    db.add(source)
    await db.flush()


async def attach(
    db: AsyncSession, *, owner_id: UUID, conversation_id: UUID | None, url: str, minio_bucket: str | None = None
) -> AttachResult:
    parsed: ParsedVideo = parse_youtube_url(url, allow_shorts=settings.YOUTUBE_ALLOW_SHORTS)
    now = _now()

    existing = await _live_source(db, owner_id, parsed.video_id, lock=True)
    if existing is not None:
        file = await FileRepository.get(db, existing.document_id)
        gone = file is None or file.status in (FileStatus.DELETING, FileStatus.DELETED)
        # A permanently failed source may be tried again once its cooldown passed: a fresh document replaces it
        # (the old file never held an object, so there is nothing to clean up in storage or the index).
        stale_permanent = (
            existing.extraction_status == "FAILED_PERMANENT" and existing.next_retry_at is not None and existing.next_retry_at <= now
        )
        if gone or stale_permanent:
            await _retire_source(db, existing, file)
            if file is not None and file.status == FileStatus.FAILED:
                await FileRepository.update_status(db, file, FileStatus.DELETING)
                await FileRepository.update_status(db, file, FileStatus.DELETED, deleted_at=now)
            existing = None
    if existing is not None:
        return AttachResult(existing, created=False, start_extraction=needs_extraction(existing, now))

    since = now - timedelta(hours=1)
    recent = (await db.execute(
        select(func.count()).select_from(YoutubeSource).where(YoutubeSource.owner_id == owner_id, YoutubeSource.created_at > since)
    )).scalar_one()
    if recent >= settings.YOUTUBE_USER_HOURLY_QUOTA:
        raise QuotaExceeded()

    document_id = new_uuid7()
    key = build_storage_key(owner_id, document_id, 1, "transcript.json")
    try:
        async with db.begin_nested():
            db.add(File(
                document_id=document_id, conversation_id=conversation_id, owner_id=owner_id,
                filename=_placeholder_name(parsed.video_id), content_type=tr.TRANSCRIPT_CONTENT_TYPE, size_bytes=0,
                storage_bucket=minio_bucket or settings.BUCKET_FILES, storage_key=key, status=FileStatus.UPLOADING,
            ))
            await db.flush()
            source = YoutubeSource(
                document_id=document_id, owner_id=owner_id, video_id=parsed.video_id, canonical_url=parsed.canonical_url,
                extraction_status="PENDING",
            )
            db.add(source)
            await db.flush()
    except IntegrityError:
        # Lost a race with a concurrent identical request: both callers end up with the same document.
        winner = await _live_source(db, owner_id, parsed.video_id, lock=False)
        if winner is None:
            raise
        return AttachResult(winner, created=False, start_extraction=needs_extraction(winner, now))
    return AttachResult(source, created=True, start_extraction=True)


async def request_retry(db: AsyncSession, *, owner_id: UUID, document_id: UUID) -> AttachResult | None:
    """User-requested retry of a failed source (re-submission is idempotent, this is the explicit button)."""
    source = (await db.execute(
        select(YoutubeSource).where(YoutubeSource.document_id == document_id, YoutubeSource.owner_id == owner_id,
                                    YoutubeSource.removed_at.is_(None)).with_for_update()
    )).scalar_one_or_none()
    if source is None:
        return None
    now = _now()
    if source.extraction_status == "FAILED_RETRYABLE" and (source.next_retry_at is None or source.next_retry_at <= now):
        return AttachResult(source, created=False, start_extraction=True)
    if source.extraction_status == "FAILED_PERMANENT" and source.next_retry_at is not None and source.next_retry_at <= now:
        return await attach(db, owner_id=owner_id, conversation_id=None, url=source.canonical_url)
    return AttachResult(source, created=False, start_extraction=needs_extraction(source, now))


async def remove(db: AsyncSession, minio: Minio, *, owner_id: UUID, document_id: UUID) -> bool:
    """Removes a source in any state. Indexed documents go through the normal delete flow (object, chunks, event);
    a source that never produced an object is retired directly. Returns False when not found / not the caller's."""
    from app.services import delete_service

    source = (await db.execute(
        select(YoutubeSource).where(YoutubeSource.document_id == document_id, YoutubeSource.owner_id == owner_id,
                                    YoutubeSource.removed_at.is_(None)).with_for_update()
    )).scalar_one_or_none()
    if source is None:
        return False
    file = await FileRepository.get(db, document_id)
    if file is not None and file.status not in (FileStatus.UPLOADING, FileStatus.READY, FileStatus.FAILED, FileStatus.DELETING, FileStatus.DELETED):
        # Mid-indexing (UPLOADED/PROCESSING): the existing lifecycle forbids deleting now. Same 409 as for documents.
        raise InvalidStateTransitionError(file.status, FileStatus.DELETING)
    source.removed_at = _now()
    db.add(source)
    await db.flush()
    if file is None or file.status in (FileStatus.DELETING, FileStatus.DELETED):
        return True
    if file.status == FileStatus.UPLOADING:                    # no object exists yet: nothing to clean in storage/index
        await FileRepository.update_status(db, file, FileStatus.FAILED, last_error_code="REMOVED")
    await db.commit()
    await delete_service.delete_file(db, minio, document_id, owner_id)
    return True


# ── extraction worker ────────────────────────────────────────────────────────────────────────────────────────────────

async def _claim(factory: async_sessionmaker, document_id: UUID) -> YoutubeSource | None:
    """Compare-and-set lease: exactly one worker proceeds for a document, however many were started."""
    now = _now()
    async with factory() as db:
        r = await db.execute(
            update(YoutubeSource)
            .where(
                YoutubeSource.document_id == document_id,
                YoutubeSource.removed_at.is_(None),
                (YoutubeSource.extraction_status == "PENDING")
                | ((YoutubeSource.extraction_status == "FAILED_RETRYABLE") & ((YoutubeSource.next_retry_at.is_(None)) | (YoutubeSource.next_retry_at <= now)))
                | ((YoutubeSource.extraction_status == "FETCHING") & ((YoutubeSource.lease_until.is_(None)) | (YoutubeSource.lease_until <= now))),
            )
            .values(extraction_status="FETCHING", attempts=YoutubeSource.attempts + 1,
                    lease_until=now + timedelta(seconds=settings.YOUTUBE_LEASE_SECONDS), error_code=None)
            .returning(YoutubeSource.document_id)
        )
        won = r.first() is not None
        await db.commit()
        if not won:
            return None
        return (await db.execute(select(YoutubeSource).where(YoutubeSource.document_id == document_id))).scalar_one()


async def _call(fn, *args):
    """Runs a blocking provider call in a thread under a wall-clock timeout."""
    try:
        return await asyncio.wait_for(asyncio.to_thread(fn, *args), timeout=settings.YOUTUBE_REQUEST_TIMEOUT_SECONDS * 3)
    except asyncio.TimeoutError:
        raise TransientDependency("timeout") from None


@dataclass
class Extracted:
    track: object
    validated: tr.ValidatedTranscript
    title: str | None


async def extract(provider: TranscriptProvider, video_id: str) -> Extracted:
    """Discovery -> selection -> fetch -> validation, with bounded in-process retries for transient failures only."""
    attempts = max(1, settings.YOUTUBE_FETCH_ATTEMPTS)
    for attempt in range(1, attempts + 1):
        try:
            tracks = await _call(provider.list_tracks, video_id)
            track = tr.select_track(tracks, settings.youtube_languages, fallback_any=settings.YOUTUBE_FALLBACK_ANY_LANGUAGE)
            raw = await _call(provider.fetch, video_id, track)
            validated = tr.validate_segments(
                raw, max_segments=settings.YOUTUBE_MAX_SEGMENTS, max_chars=settings.YOUTUBE_MAX_TRANSCRIPT_CHARS,
                max_duration=settings.YOUTUBE_MAX_DURATION_SECONDS,
            )
            try:
                title = await _call(provider.video_title, video_id)
            except TranscriptError:
                title = None
            return Extracted(track, validated, title)
        except TranscriptError as exc:
            if exc.retryable and exc.backoff_in_process and attempt < attempts:
                delay = settings.YOUTUBE_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                await asyncio.sleep(delay + random.uniform(0, delay / 2))
                continue
            raise
    raise AssertionError("unreachable")


async def _record_failure(factory: async_sessionmaker, document_id: UUID, exc: TranscriptError | Exception) -> None:
    code = exc.code if isinstance(exc, TranscriptError) else "INTERNAL_ERROR"
    retryable = exc.retryable if isinstance(exc, TranscriptError) else True
    now = _now()
    cooldown = settings.YOUTUBE_RETRY_COOLDOWN_SECONDS if retryable else settings.YOUTUBE_PERMANENT_RETRY_COOLDOWN_SECONDS
    async with factory() as db:
        source = (await db.execute(select(YoutubeSource).where(YoutubeSource.document_id == document_id).with_for_update())).scalar_one_or_none()
        if source is None or source.removed_at is not None:
            await db.commit()
            return
        backoff = min(cooldown * (2 ** max(0, source.attempts - 1)), 900) if retryable else cooldown
        source.extraction_status = "FAILED_RETRYABLE" if retryable else "FAILED_PERMANENT"
        source.error_code = code
        source.lease_until = None
        source.next_retry_at = now + timedelta(seconds=backoff)
        db.add(source)
        if not retryable:
            file = await FileRepository.get(db, document_id)
            if file is not None and file.status == FileStatus.UPLOADING:
                await FileRepository.update_status(db, file, FileStatus.FAILED, last_error_code=code)
        await db.commit()
    logger.warning("youtube_extraction_failed", document_id=str(document_id), code=code, retryable=retryable)


async def _store_success(factory: async_sessionmaker, minio: Minio, source: YoutubeSource, ex: Extracted) -> None:
    track = ex.track
    sha = tr.transcript_sha256(source.video_id, track, ex.validated.segments)
    async with factory() as db:
        file = await FileRepository.get_for_update(db, source.document_id)
        row = (await db.execute(select(YoutubeSource).where(YoutubeSource.document_id == source.document_id).with_for_update())).scalar_one()
        if file is None or row.removed_at is not None or file.status != FileStatus.UPLOADING:
            await db.commit()                       # removed while fetching, or already stored: nothing to do
            return
        version = 1
        data = tr.build_canonical(
            video_id=source.video_id, canonical_url=source.canonical_url, title=ex.title, track=track,
            segments=ex.validated.segments, provider="youtube-transcript-api",
            transcript_version=version,
        )
        file_sha = hashlib.sha256(data).hexdigest()
        # Object first (deterministic key, so a retry after a crash simply overwrites it), then the DB in one transaction.
        result = await asyncio.to_thread(
            minio.put_object, file.storage_bucket, file.storage_key, io.BytesIO(data), len(data),
            content_type=tr.TRANSCRIPT_CONTENT_TYPE,
        )
        etag = getattr(result, "etag", None) or file_sha
        await FileVersionRepository.insert(
            db, document_id=file.document_id, version=version, storage_key=file.storage_key, etag=etag,
            size_bytes=len(data), checksum_sha256=file_sha,
        )
        await FileRepository.update_status(
            db, file, FileStatus.UPLOADED, etag=etag, size_bytes=len(data), checksum_sha256=file_sha,
            filename=display_filename(ex.title, source.video_id),
        )
        await OutboxRepository.insert(
            db, aggregate_id=file.document_id, event_type="file.uploaded",
            payload=_build_file_uploaded_payload(file, SimpleNamespace(etag=etag)),
        )
        row.title = ex.title
        row.language, row.language_name, row.is_generated = track.language_code, track.language, track.is_generated
        row.provider = "youtube-transcript-api"
        row.transcript_version, row.transcript_sha256 = version, sha
        row.segment_count, row.duration_seconds = len(ex.validated.segments), ex.validated.duration_seconds
        row.extraction_status, row.error_code, row.lease_until, row.next_retry_at = "FETCHED", None, None, None
        row.fetched_at = _now()
        db.add(row)
        await db.commit()
    logger.info("youtube_transcript_stored", document_id=str(source.document_id), segments=len(ex.validated.segments),
                language=track.language_code, generated=track.is_generated)


async def run_extraction(factory: async_sessionmaker, minio: Minio, provider: TranscriptProvider, document_id: UUID) -> None:
    """Background entry point. Never raises: every outcome is recorded on the source row."""
    try:
        async with _sem():
            source = await _claim(factory, document_id)
            if source is None:
                return                                  # another worker owns it, it is finished, or it was removed
            try:
                ex = await extract(provider, source.video_id)
            except TranscriptError as exc:
                await _record_failure(factory, document_id, exc)
                return
            try:
                await _store_success(factory, minio, source, ex)
            except Exception as exc:  # noqa: BLE001 - storage/DB failure after a good fetch: retryable, nothing half-written
                logger.error("youtube_store_failed", document_id=str(document_id), error_type=type(exc).__name__)
                await _record_failure(factory, document_id, TransientDependency("storage"))
    except Exception as exc:  # noqa: BLE001
        logger.error("youtube_worker_crashed", document_id=str(document_id), error_type=type(exc).__name__)


# ── read model ───────────────────────────────────────────────────────────────────────────────────────────────────────

def view(source: YoutubeSource, file: File) -> dict:
    """What the API returns. `ready` is true only when extraction finished AND indexing completed."""
    now = _now()
    failed = source.extraction_status in ("FAILED_RETRYABLE", "FAILED_PERMANENT")
    indexing = file.status.value
    ready = source.extraction_status == "FETCHED" and file.status == FileStatus.READY
    if source.extraction_status == "FETCHED" and file.status == FileStatus.FAILED:
        failed, code = True, file.last_error_code or "INDEXING_ERROR"
    else:
        code = source.error_code
    retry_after = None
    if source.extraction_status == "FAILED_RETRYABLE" and source.next_retry_at and source.next_retry_at > now:
        retry_after = int((source.next_retry_at - now).total_seconds()) + 1
    return {
        "document_id": source.document_id,
        "video_id": source.video_id,
        "canonical_url": source.canonical_url,
        "title": source.title,
        "language": source.language,
        "is_generated": source.is_generated,
        "transcript_version": source.transcript_version,
        "segment_count": source.segment_count,
        "duration_seconds": source.duration_seconds,
        "extraction_status": source.extraction_status,
        "indexing_status": indexing,
        "ready": ready,
        "error_code": code if failed else None,
        "message": SAFE_MESSAGES.get(code or "", SAFE_MESSAGES["INTERNAL_ERROR"]) if failed else None,
        "retryable": source.extraction_status == "FAILED_RETRYABLE",
        "retry_after_seconds": retry_after,
    }
