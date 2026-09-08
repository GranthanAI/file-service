from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user_id, get_db, require_permission
from app.api.schemas.permission import GrantPermissionRequest, PermissionResponse
from app.core.enums import PermissionLevel
from app.repositories.file_permission_repository import FilePermissionRepository

router = APIRouter(tags=["permissions"])


@router.put(
    "/files/{document_id}/permissions",
    response_model=PermissionResponse,
    # Route-level dependency (not a param inside the function): FastAPI
    # resolves document_id from the path for it, runs the OWNER check before
    # the handler body executes at all, and discards the returned File row —
    # the handler doesn't need it, only the pass/raise matters here.
    dependencies=[Depends(require_permission(PermissionLevel.OWNER))],
)
async def grant_permission(
    document_id: UUID,
    body: GrantPermissionRequest,
    granted_by: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
) -> PermissionResponse:
    """docs/LLD.md §5.9 — requires OWNER (the file's owner_id, since
    file_permissions has no OWNER-level grant rows of its own — see §2.4)."""
    # upsert: a repeat PUT for the same (document_id, principal_id) updates
    # the existing grant's access_level in place rather than erroring or
    # creating a duplicate row (see file_permission_repository.upsert).
    grant = await FilePermissionRepository.upsert(
        db,
        document_id=document_id,
        principal_id=body.principal_id,
        access_level=body.access_level,
        granted_by=granted_by,
        principal_type=body.principal_type.value,
    )
    return PermissionResponse(
        document_id=grant.document_id,
        principal_id=grant.principal_id,
        principal_type=grant.principal_type,
        access_level=grant.access_level,
        granted_by=grant.granted_by,
    )
