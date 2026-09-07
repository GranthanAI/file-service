from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import FileStatus, PresignMethod
from app.core.exceptions import (
    AlreadyCompletedError,
    ContentTypeMismatchError,
    FileRecordNotFoundError,
    ObjectNotFoundError,
    PermissionDeniedError,
    PresignExpiredError,
    SizeMismatchError,
)
from app.repositories.file_repository import FileRepository
from app.repositories.file_version_repository import FileVersionRepository
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.upload_job_repository import UploadJobRepository
from app.storage.key_builder import build_storage_key
from app.storage.object_verifier import ObjectVerification, verify_uploaded_object
from app.storage.presign import presign_post_policy, presign_put
from app.utils.id_generator import new_uuid7
from app.validation import content_type as content_type_validation
from app.validation import size_limits


@dataclass(frozen=True)
class InitiateUploadResult:
    document_id: UUID
    upload_url: str
    upload_method: PresignMethod
    form_data: dict[str, str] | None
    expires_in: int


def _target_bucket() -> str:
    """docs/LLD.md §4.1: storage_bucket is ALWAYS the quarantine bucket when
    AV_SCAN_ENABLED — the object only ever lands in graphgpt-files directly
    when scanning is disabled (dev/local only, see docs/LLD.md §4.5)."""
    return settings.BUCKET_QUARANTINE if settings.AV_SCAN_ENABLED else settings.BUCKET_FILES


def _build_file_uploaded_payload(file, verification: ObjectVerification) -> dict[str, Any]:
    """docs/HLD.md §13 file.uploaded payload shape. event_id/event_type/
    schema version are added by the outbox worker (Phase 5) from the
    outbox_events row itself at publish time, not duplicated here."""
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


async def initiate_upload(
    db: AsyncSession,
    minio_client: Minio,
    *,
    owner_id: UUID,
    conversation_id: UUID | None,
    filename: str,
    content_type: str,
    size_bytes: int,
) -> InitiateUploadResult:
    """docs/LLD.md §4.1 initiate_upload — steps 1-8."""
    content_type_validation.assert_allowed(content_type)
    size_limits.assert_within_limit(size_bytes)

    document_id = new_uuid7()
    bucket = _target_bucket()
    storage_key = build_storage_key(owner_id, document_id, version=1, filename=filename)

    # Steps 5-6 in one DB transaction (docs/LLD.md §4.1: an upload_jobs row
    # without a matching files row, or vice versa, would break /complete).
    # get_db commits at the end of the request; both inserts share that txn.
    await FileRepository.insert(
        db,
        document_id=document_id,
        conversation_id=conversation_id,
        owner_id=owner_id,
        filename=filename,
        content_type=content_type,
        size_bytes=size_bytes,
        storage_bucket=bucket,
        storage_key=storage_key,
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
            minio_client, bucket, storage_key, content_type=content_type, max_size=size_bytes, expiry=ttl
        )
    else:
        upload_url = presign_put(minio_client, bucket, storage_key, expiry=ttl)
        form_data = None

    return InitiateUploadResult(
        document_id=document_id,
        upload_url=upload_url,
        upload_method=presign_method,
        form_data=form_data,
        expires_in=settings.UPLOAD_PRESIGN_TTL_SECONDS,
    )


async def complete_upload(
    db: AsyncSession,
    minio_client: Minio,
    *,
    document_id: UUID,
    requester_id: UUID,
    client_checksum: str | None = None,
):
    """docs/LLD.md §4.1 complete_upload — the one authoritative /complete
    implementation, identical whether AV_SCAN_ENABLED is true or false."""
    file = await FileRepository.get_for_update(db, document_id)
    if file is None:
        raise FileRecordNotFoundError(document_id)

    if file.owner_id != requester_id:
        raise PermissionDeniedError(document_id, requester_id)

    if file.status != FileStatus.UPLOADING:
        # Idempotent: already completed. A mismatched client_checksum means
        # the caller thinks a different upload happened — that's the "409
        # already completed with different terms" case (docs/LLD.md §5.2);
        # anything else (repeat call, no checksum) returns current state.
        if client_checksum is not None and file.etag is not None and client_checksum != file.etag:
            raise AlreadyCompletedError(document_id)
        return file

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
    except (ObjectNotFoundError, SizeMismatchError, ContentTypeMismatchError) as e:
        await FileRepository.update_status(db, file, FileStatus.FAILED, last_error_code=type(e).__name__)
        # get_db's dependency wrapper rolls back the whole request transaction
        # on any exception (correct default for genuinely unexpected errors)
        # — but this exception is deliberately re-raised so the registered
        # handler can turn it into a 422, and the FAILED status write above
        # must survive that. Commit explicitly before re-raising rather than
        # letting it be silently discarded.
        await db.commit()
        raise

    await FileVersionRepository.insert(
        db,
        document_id=document_id,
        version=1,
        storage_key=file.storage_key,
        etag=verification.etag,
        size_bytes=verification.size,
    )
    await UploadJobRepository.mark_completed(db, job)

    if settings.AV_SCAN_ENABLED:
        # No outbox row here — file.uploaded is emitted only by
        # quarantine_service (Phase 6) once the object is promoted out of
        # the quarantine bucket (docs/LLD.md §4.1, §4.5).
        await FileRepository.update_status(db, file, FileStatus.UPLOADED, etag=verification.etag)
    else:
        await FileRepository.update_status(db, file, FileStatus.UPLOADED, etag=verification.etag)
        await OutboxRepository.insert(
            db,
            aggregate_id=document_id,
            event_type="file.uploaded",
            payload=_build_file_uploaded_payload(file, verification),
        )

    return file
