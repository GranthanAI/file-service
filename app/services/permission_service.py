from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import FileStatus, PermissionLevel
from app.core.exceptions import FileRecordNotFoundError, PermissionDeniedError
from app.models.file import File
from app.repositories.file_permission_repository import FilePermissionRepository
from app.repositories.file_repository import FileRepository

# docs/LLD.md §4.3 — a simple total ordering avoids a permission-matrix table.
LEVEL_RANK: dict[PermissionLevel, int] = {
    PermissionLevel.VIEWER: 1,
    PermissionLevel.EDITOR: 2,
    PermissionLevel.OWNER: 3,
}


async def assert_access(db: AsyncSession, document_id: UUID, principal_id: UUID, required: PermissionLevel) -> File:
    """docs/LLD.md §2.4, §4.3 — USER-only scope: file_permission_repository
    resolves a direct USER grant only, no team-membership resolution.

    A soft-deleted file (status DELETED) is treated as not found here, same
    as a missing row, matching the 404 behavior documented for
    GET /files/{document_id} (docs/LLD.md §5.3) — the caller must not be able
    to distinguish "never existed" from "deleted" via a permission check.

    Returns the resolved File row so callers that need it anyway (e.g.
    file_service.get_file) avoid a second SELECT.
    """
    # Step 1: does the document even exist (and is it still "live")?
    file = await FileRepository.get(db, document_id)
    if file is None or file.status == FileStatus.DELETED:
        raise FileRecordNotFoundError(document_id)

    # Step 2: the owner short-circuits straight through — no file_permissions
    # row needed, and no rank comparison to do.
    if file.owner_id == principal_id:
        return file

    # Step 3: anyone else needs an explicit grant whose access_level ranks at
    # or above what this route requires (e.g. an EDITOR grant satisfies a
    # VIEWER requirement, but not the other way around).
    grant = await FilePermissionRepository.get(db, document_id, principal_id)
    if grant is None or LEVEL_RANK[grant.access_level] < LEVEL_RANK[required]:
        raise PermissionDeniedError(document_id, principal_id)

    return file
