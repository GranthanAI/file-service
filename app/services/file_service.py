from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import FileStatus, PermissionLevel
from app.models.file import File
from app.repositories.file_repository import FileRepository
from app.services import permission_service


async def get_file(db: AsyncSession, document_id: UUID, requester_id: UUID) -> File:
    """docs/LLD.md §5.3 — requires at least VIEWER access (owner implicitly
    qualifies, see permission_service.assert_access)."""
    # assert_access both enforces the permission AND fetches the row, so
    # there's nothing left to do here beyond handing the result back — a
    # failed check raises before returning, short-circuiting the caller.
    return await permission_service.assert_access(db, document_id, requester_id, PermissionLevel.VIEWER)


async def list_files(
    db: AsyncSession,
    *,
    requester_id: UUID,
    conversation_id: UUID | None = None,
    status: FileStatus | None = None,
    cursor: UUID | None = None,
    limit: int = 20,
) -> list[File]:
    """docs/LLD.md §5.4 — keyset pagination on document_id (UUIDv7). Scoped to
    files owned by the requester, matching the owner_id filter already
    implemented by file_repository.list_paginated; listing files shared via
    file_permissions is not part of this endpoint's documented scope."""
    return await FileRepository.list_paginated(
        db,
        owner_id=requester_id,
        conversation_id=conversation_id,
        status=status,
        cursor=cursor,
        limit=limit,
    )
