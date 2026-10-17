from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, require_internal_service
from app.api.schemas.permission import PermissionCheckRequest, PermissionCheckResponse
from app.repositories.file_permission_repository import FilePermissionRepository

# Service-to-service only. The API gateway must never proxy /internal/**.
router = APIRouter(prefix="/internal", tags=["internal"], dependencies=[Depends(require_internal_service)])


@router.post("/files/permissions/check", response_model=PermissionCheckResponse)
async def check_permissions(
    body: PermissionCheckRequest, db: AsyncSession = Depends(get_db)
) -> PermissionCheckResponse:
    """Retrieval HLD 16.2 - returns the subset of document_ids that user_id may
    read. Unauthorized and nonexistent ids are indistinguishable (omitted)."""
    ids = await FilePermissionRepository.authorized_document_ids(db, body.user_id, body.document_ids)
    return PermissionCheckResponse(authorized_document_ids=ids)
