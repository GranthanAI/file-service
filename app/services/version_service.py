"""docs/HLD.md §16, docs/LLD.md §2.3 — creating a new version of an existing
file: a NEW object key (v{n+1}/original.ext), not an in-place overwrite,
tracked as a new file_versions row. Mirrors upload_service.py's
initiate_upload/complete_upload shape (same presign-then-verify pattern) but
scoped to an existing document_id instead of minting a new one.

Service-layer only — no HTTP route. docs/IMPLEMENTATION_PLAN.md Phase 8's
task list names this module but no corresponding router, and the LLD's own
API table (§5.1-§5.9) documents no versioning endpoint at all; inventing an
HTTP contract nothing in the LLD/HLD specifies would be scope creep past
what's actually asked. These functions are exercised directly (service-level
tests), the same way lifecycle_service/permission_service are.

Scoped to AV_SCAN_ENABLED=false. The existing, already-tested
lifecycle_service.ALLOWED_TRANSITIONS (docs/LLD.md §4.2) has no edge from
READY back to UPLOADED/SCANNING — only UPLOADING/SCANNING can reach UPLOADED
at all. A new version therefore cannot legally re-enter the quarantine
AV-scan path (docs/LLD.md §4.5) without extending that state machine, which
is out of this module's scope to change. Mirrors the same phased narrowing
this codebase already used once before: upload_service.py was itself
explicitly AV-scan-disabled-only in Phase 3, with quarantine added
separately in Phase 6.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import FileStatus, PermissionLevel, PresignMethod
from app.core.exceptions import (
    ContentTypeMismatchError,
    FileRecordNotFoundError,
    ObjectNotFoundError,
    PermissionDeniedError,
    PresignExpiredError,
    SizeMismatchError,
    VersioningNotAllowedError,
)
from app.models.file import File
from app.repositories.file_repository import FileRepository
from app.repositories.file_version_repository import FileVersionRepository
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.upload_job_repository import UploadJobRepository
from app.services import permission_service
from app.storage.key_builder import build_storage_key
from app.storage.object_verifier import ObjectVerification, verify_uploaded_object
from app.storage.presign import presign_post_policy, presign_put
from app.validation import content_type as content_type_validation
from app.validation import size_limits


@dataclass(frozen=True)
class InitiateVersionResult:
    document_id: UUID
    version: int
    upload_url: str
    upload_method: PresignMethod
    form_data: dict[str, str] | None
    expires_in: int


def _build_file_uploaded_payload(file: File, verification: ObjectVerification) -> dict[str, Any]:
    """docs/HLD.md §13 — reuses the file.uploaded shape for a new version's
    completion too: no separate event type is documented for versioning
    (docs/LLD.md/HLD.md are both silent on this), and Retrieval Service
    needs to re-index the new version the same way it indexes any other
    file.uploaded — so this is the same signal, not a new contract."""
    return {
        "document_id": str(file.document_id),
        "conversation_id": str(file.conversation_id) if file.conversation_id else None,
        "owner_id": str(file.owner_id),
        "filename": file.filename,
        "content_type": file.content_type,
        "storage": {
            "provider": "minio",
            "bucket": file.storage_bucket,
            "key": file.storage_key,
            "etag": verification.etag,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def initiate_new_version(
    db: AsyncSession,
    minio_client: Minio,
    *,
    document_id: UUID,
    requester_id: UUID,
    filename: str,
    content_type: str,
    size_bytes: int,
) -> InitiateVersionResult:
    """docs/HLD.md §16/§2.3 — requires the file to currently be READY (the
    previous version fully processed); EDITOR access (mutates the file's
    content without granting/revoking access or deleting anything, same
    reasoning as retry_service's permission choice)."""
    file = await permission_service.assert_access(db, document_id, requester_id, PermissionLevel.EDITOR)

    if file.status != FileStatus.READY:
        raise VersioningNotAllowedError(document_id, file.status)
    if settings.AV_SCAN_ENABLED:
        raise VersioningNotAllowedError(document_id, file.status)

    content_type_validation.assert_allowed(content_type)
    size_limits.assert_within_limit(size_bytes)

    next_version = file.current_version + 1
    storage_key = build_storage_key(file.owner_id, document_id, version=next_version, filename=filename)

    # Repoints files.storage_key/current_version at the prospective new
    # version before any bytes exist — same accepted transient-inconsistency
    # window FileRepository.insert already uses for a brand-new v1 row (see
    # set_pending_version's own docstring).
    await FileRepository.set_pending_version(
        db, file, storage_key=storage_key, storage_bucket=settings.BUCKET_FILES, current_version=next_version
    )

    expires_at = datetime.now(timezone.utc) + timedelta(seconds=settings.UPLOAD_PRESIGN_TTL_SECONDS)
    presign_method = PresignMethod.POST if settings.UPLOAD_STRICT_POLICY else PresignMethod.PUT
    await UploadJobRepository.insert(
        db,
        document_id=document_id,
        presign_method=presign_method.value,
        expected_size=size_bytes,
        expected_content_type=content_type,
        expires_at=expires_at,
    )

    ttl = timedelta(seconds=settings.UPLOAD_PRESIGN_TTL_SECONDS)
    if presign_method is PresignMethod.POST:
        upload_url, form_data = presign_post_policy(
            minio_client, settings.BUCKET_FILES, storage_key, content_type=content_type, max_size=size_bytes, expiry=ttl
        )
    else:
        upload_url = presign_put(minio_client, settings.BUCKET_FILES, storage_key, expiry=ttl)
        form_data = None

    return InitiateVersionResult(
        document_id=document_id,
        version=next_version,
        upload_url=upload_url,
        upload_method=presign_method,
        form_data=form_data,
        expires_in=settings.UPLOAD_PRESIGN_TTL_SECONDS,
    )


async def complete_new_version(
    db: AsyncSession, minio_client: Minio, *, document_id: UUID, requester_id: UUID
) -> File:
    """docs/HLD.md §16/§2.3 — verifies the new version's object, records it
    in file_versions, and re-signals Retrieval Service via file.uploaded.
    Lands on PROCESSING, not READY (docs/LLD.md §4.2's documented
    "re-processing (e.g. reindex)" edge) — READY is set only by
    processing_status_consumer.py (Phase 7) once Retrieval Service confirms
    the new version's own file.processing.completed, exactly like the
    original upload path."""
    file = await FileRepository.get_for_update(db, document_id)
    if file is None:
        raise FileRecordNotFoundError(document_id)
    if file.owner_id != requester_id:
        raise PermissionDeniedError(document_id, requester_id)

    if file.status == FileStatus.PROCESSING:
        # Idempotent: this version's completion already went through.
        return file
    if file.status != FileStatus.READY:
        raise VersioningNotAllowedError(document_id, file.status)

    job = await UploadJobRepository.get_pending(db, document_id)
    if job is None or datetime.now(timezone.utc) >= job.expires_at:
        raise PresignExpiredError(document_id)

    try:
        verification = verify_uploaded_object(
            minio_client,
            file.storage_bucket,
            file.storage_key,
            expected_size=job.expected_size,
            expected_content_type=job.expected_content_type,
        )
    except (ObjectNotFoundError, SizeMismatchError, ContentTypeMismatchError):
        # READY has no valid edge to FAILED (docs/LLD.md §4.2) — a failed
        # new-version attempt must not take down the file's existing,
        # already-working previous version. Revert the pointer fields to the
        # last known-good version rather than leaving them aimed at bytes
        # that don't exist; files.status never changes, so no transition is
        # even attempted here.
        previous_versions = await FileVersionRepository.list_for_document(db, document_id)
        if previous_versions:
            last_good = previous_versions[-1]
            await FileRepository.set_pending_version(
                db,
                file,
                storage_key=last_good.storage_key,
                storage_bucket=settings.BUCKET_FILES,
                current_version=last_good.version,
            )
        raise

    await FileVersionRepository.insert(
        db,
        document_id=document_id,
        version=file.current_version,
        storage_key=file.storage_key,
        etag=verification.etag,
        size_bytes=verification.size,
    )
    await UploadJobRepository.mark_completed(db, job)
    await FileRepository.update_status(db, file, FileStatus.PROCESSING, etag=verification.etag)
    await OutboxRepository.insert(
        db,
        aggregate_id=document_id,
        event_type="file.uploaded",
        payload=_build_file_uploaded_payload(file, verification),
    )

    return file
