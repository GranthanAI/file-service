"""Phase 6 exit-criteria tests (docs/IMPLEMENTATION_PLAN.md): the quarantine
promote/reject flow (docs/LLD.md §4.5/§6.6) against real Postgres + MinIO.

Uses a small deterministic fake Scanner (flags the literal EICAR test
string) rather than a live ClamAV daemon — `av_scanner.ClamdScanner`'s wire
protocol is already covered in isolation by tests/unit/test_av_scanner.py
(mocking `clamd`), so this file focuses on proving quarantine_service's own
orchestration (CopyObject/RemoveObject, status transitions, outbox events)
against real storage and a real database, without the extra flakiness/slow
image-pull cost of standing up a real ClamAV container in every test run.
"""

import io
import uuid

from minio import Minio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import FileStatus
from app.models.outbox import OutboxEvent
from app.repositories.file_repository import FileRepository
from app.services.av_scanner import ScanVerdict
from app.services.quarantine_service import process_object_created
from app.utils.id_generator import new_uuid7

EICAR = b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"
CLEAN_PDF = b"%PDF-1.4 a real, clean, boring test PDF"

# _promote_clean_object always targets settings.BUCKET_FILES (docs/LLD.md
# §4.5 — promotion has one, fixed destination), so these tests use the real
# configured bucket names rather than arbitrary ones, matching
# tests/integration/test_storage.py::test_bucket_bootstrap_is_idempotent's
# own approach of creating settings.BUCKET_FILES/BUCKET_QUARANTINE directly
# on the ephemeral test MinIO.
BUCKET_FILES = settings.BUCKET_FILES
BUCKET_QUARANTINE = settings.BUCKET_QUARANTINE


class _EicarScanner:
    """A deterministic Scanner double: EICAR bytes are "infected", anything
    else is "clean" — this is literally how EICAR detection works in real
    AV engines too (it's a fixed, publicly known byte string designed to be
    matched exactly), so this isn't a shortcut around what ClamAV itself
    would report for these two fixtures."""

    async def scan(self, data: bytes) -> ScanVerdict:
        if data == EICAR:
            return ScanVerdict(clean=False, signature="Eicar-Signature")
        return ScanVerdict(clean=True)


def _ensure_buckets(minio_client: Minio) -> None:
    for bucket in (BUCKET_FILES, BUCKET_QUARANTINE):
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)


async def _insert_uploaded_file(db: AsyncSession, minio_client: Minio, *, content: bytes, content_type: str) -> tuple:
    document_id = new_uuid7()
    owner_id = uuid.uuid4()
    key = f"{owner_id}/{document_id}/v1/original.pdf"

    minio_client.put_object(BUCKET_QUARANTINE, key, data=io.BytesIO(content), length=len(content))

    file_obj = await FileRepository.insert(
        db,
        document_id=document_id,
        conversation_id=None,
        owner_id=owner_id,
        filename="physics.pdf",
        content_type=content_type,
        size_bytes=len(content),
        storage_bucket=BUCKET_QUARANTINE,
        storage_key=key,
    )
    await FileRepository.update_status(db, file_obj, FileStatus.UPLOADED, etag="fake-etag")
    await db.commit()
    return file_obj, key


async def test_clean_object_is_promoted_and_file_uploaded_is_published(
    db_session: AsyncSession, minio_client: Minio
) -> None:
    _ensure_buckets(minio_client)
    file_obj, key = await _insert_uploaded_file(db_session, minio_client, content=CLEAN_PDF, content_type="application/pdf")

    await process_object_created(db_session, minio_client, _EicarScanner(), bucket=BUCKET_QUARANTINE, key=key)

    assert minio_client.bucket_exists(BUCKET_FILES)
    stat = minio_client.stat_object(BUCKET_FILES, key)
    assert stat.size == len(CLEAN_PDF)

    quarantine_objects = list(minio_client.list_objects(BUCKET_QUARANTINE, prefix=key))
    assert quarantine_objects == []

    refreshed = await FileRepository.get(db_session, file_obj.document_id)
    assert refreshed.status == FileStatus.UPLOADED
    assert refreshed.storage_bucket == BUCKET_FILES
    assert refreshed.checksum_sha256 is not None
    assert len(refreshed.checksum_sha256) == 64  # SHA-256 hex digest

    events = (
        (await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id == file_obj.document_id)))
        .scalars()
        .all()
    )
    uploaded_events = [e for e in events if e.event_type == "file.uploaded"]
    assert len(uploaded_events) == 1
    assert not any(e.event_type == "file.scan_failed" for e in events)


async def test_infected_object_ends_failed_and_only_scan_failed_is_published(
    db_session: AsyncSession, minio_client: Minio
) -> None:
    _ensure_buckets(minio_client)
    file_obj, key = await _insert_uploaded_file(db_session, minio_client, content=EICAR, content_type="application/pdf")

    await process_object_created(db_session, minio_client, _EicarScanner(), bucket=BUCKET_QUARANTINE, key=key)

    quarantine_objects = list(minio_client.list_objects(BUCKET_QUARANTINE, prefix=key))
    assert quarantine_objects == []

    files_objects = list(minio_client.list_objects(BUCKET_FILES, prefix=key))
    assert files_objects == []  # never promoted

    refreshed = await FileRepository.get(db_session, file_obj.document_id)
    assert refreshed.status == FileStatus.FAILED
    assert refreshed.last_error_code == "AV_SCAN_INFECTED"

    events = (
        (await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id == file_obj.document_id)))
        .scalars()
        .all()
    )
    event_types = {e.event_type for e in events}
    assert event_types == {"file.scan_failed"}
    assert "file.uploaded" not in event_types  # never emitted for this document_id


async def test_redelivered_event_for_an_already_scanned_file_is_a_noop(
    db_session: AsyncSession, minio_client: Minio
) -> None:
    """docs/LLD.md §3.7 at-least-once tolerance: a duplicate ObjectCreated
    delivery for the same quarantine key, after the first delivery already
    promoted the object out of quarantine, must not re-run the scan or
    publish a second event. By the second call, the file's storage_bucket
    row has already moved to BUCKET_FILES, so the lookup by
    (BUCKET_QUARANTINE, key) simply finds nothing (an "unknown object"
    no-op) rather than a status mismatch — either way, the event is safely
    dropped instead of double-processed."""
    _ensure_buckets(minio_client)
    file_obj, key = await _insert_uploaded_file(db_session, minio_client, content=CLEAN_PDF, content_type="application/pdf")

    await process_object_created(db_session, minio_client, _EicarScanner(), bucket=BUCKET_QUARANTINE, key=key)
    await process_object_created(db_session, minio_client, _EicarScanner(), bucket=BUCKET_QUARANTINE, key=key)

    events = (
        (await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id == file_obj.document_id)))
        .scalars()
        .all()
    )
    assert len(events) == 1  # not duplicated by the redelivered event
