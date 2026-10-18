from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user_id, get_db
from app.api.schemas.file import FileStatusResponse
from app.core.enums import FileStatus
from app.services import file_service

router = APIRouter(tags=["status"])

# User-facing reason per processing error code. The stored `last_error_message` can be a
# raw exception string from the ingestion pipeline (hosts, paths, library internals), so
# it is never returned to clients; only these fixed messages are.
_SAFE_ERROR_MESSAGES = {
    "NO_EXTRACTABLE_TEXT": "No text could be extracted from this document (it may be a scanned or image-only file).",
    "PARSER_ERROR": "The document could not be read. It may be damaged or in an unsupported format.",
    "DOWNLOAD_ERROR": "The uploaded file could not be read from storage. Try again.",
    "EMBEDDING_ERROR": "The document could not be indexed right now. Try again later.",
    "INDEXING_ERROR": "The document could not be indexed right now. Try again later.",
}
_GENERIC_ERROR_MESSAGE = "Processing failed. Try again."


def safe_error_message(error_code: str | None) -> str:
    return _SAFE_ERROR_MESSAGES.get(error_code or "", _GENERIC_ERROR_MESSAGE)


@router.get("/files/{document_id}/status", response_model=FileStatusResponse)
async def get_file_status(
    document_id: UUID,
    requester_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
) -> FileStatusResponse:
    """docs/LLD.md §5.6."""
    # Reuses file_service.get_file rather than a separate lookup — it already
    # enforces VIEWER+ access and 404s on unknown/soft-deleted documents, and
    # status/retry_count are just two fields off the same row.
    file = await file_service.get_file(db, document_id, requester_id)
    failed = file.status == FileStatus.FAILED
    return FileStatusResponse(
        document_id=file.document_id,
        status=file.status,
        retry_count=file.retry_count,
        error_code=file.last_error_code if failed else None,
        error_message=safe_error_message(file.last_error_code) if failed else None,
    )
