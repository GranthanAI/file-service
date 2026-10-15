from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import FileStatus, PermissionLevel
from app.core.exceptions import RetryNotAllowedError
from app.models.file import File
from app.repositories.file_repository import FileRepository
from app.repositories.outbox_repository import OutboxRepository
from app.services import permission_service


def _build_file_processing_retry_payload(file: File) -> dict[str, Any]:
    """docs/LLD.md §4.4 — the exact payload shape shown there."""
    return {
        "document_id": str(file.document_id),
        "content_version": file.current_version,
        "retry_count": file.retry_count + 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def retry_upload(db: AsyncSession, document_id: UUID, requester_id: UUID) -> File:
    """docs/LLD.md §4.4 — FAILED -> PROCESSING, republish
    file.processing_retry, increment retry_count. The original object in
    MinIO is untouched — this only re-signals Retrieval Service to retry.

    Deliberately never sets READY itself (docs/LLD.md §3.6's closing note):
    after this call the file sits in PROCESSING until Retrieval Service
    consumes file.processing_retry and eventually emits
    file.processing.completed or file.processing.failed, handled
    exclusively by processing_status_consumer.py (Phase 7) — exactly like
    the original file.uploaded-triggered processing path.
    """
    # EDITOR, not OWNER: retry mutates processing state (closer to "editing"
    # the file's pipeline status) but neither grants/revokes access nor
    # deletes anything — docs/LLD.md §5.7 doesn't list a permission error at
    # all, but §9's blanket rule ("every read/write route resolves through
    # permission_service.assert_access") still applies.
    file = await permission_service.assert_access(db, document_id, requester_id, PermissionLevel.EDITOR)

    if file.status != FileStatus.FAILED:
        raise RetryNotAllowedError(document_id, file.status)

    await FileRepository.update_status(db, file, FileStatus.PROCESSING)
    await OutboxRepository.insert(
        db,
        aggregate_id=document_id,
        event_type="file.processing_retry",
        payload=_build_file_processing_retry_payload(file),
    )
    # Built the payload above using file.retry_count + 1 BEFORE this
    # increments the real column — the event and the DB end up agreeing on
    # the same new count without a second read.
    await FileRepository.increment_retry_count(db, file)

    return file
