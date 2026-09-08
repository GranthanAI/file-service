from uuid import UUID

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user_id, get_db
from app.api.schemas.file import FileStatusResponse
from app.services import retry_service

router = APIRouter(tags=["retry"])


@router.post("/files/{document_id}/retry", response_model=FileStatusResponse, status_code=status.HTTP_202_ACCEPTED)
async def retry_upload(
    document_id: UUID,
    requester_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
) -> FileStatusResponse:
    """docs/LLD.md §5.7 — reuses FileStatusResponse rather than a dedicated
    schema since the response shape (document_id, status, retry_count) is
    identical to GET .../status."""
    file = await retry_service.retry_upload(db, document_id, requester_id)
    return FileStatusResponse(document_id=file.document_id, status=file.status, retry_count=file.retry_count)
