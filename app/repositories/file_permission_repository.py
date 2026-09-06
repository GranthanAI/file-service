from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import PermissionLevel, PrincipalType
from app.models.file_permission import FilePermission


class FilePermissionRepository:
    @staticmethod
    async def get(
        db: AsyncSession,
        document_id: UUID,
        principal_id: UUID,
        principal_type: str = PrincipalType.USER.value,
    ) -> FilePermission | None:
        result = await db.execute(
            select(FilePermission).where(
                FilePermission.document_id == document_id,
                FilePermission.principal_id == principal_id,
                FilePermission.principal_type == principal_type,
            )
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def upsert(
        db: AsyncSession,
        *,
        document_id: UUID,
        principal_id: UUID,
        access_level: PermissionLevel,
        granted_by: UUID,
        principal_type: str = PrincipalType.USER.value,
    ) -> FilePermission:
        existing = await FilePermissionRepository.get(db, document_id, principal_id, principal_type)
        if existing is not None:
            existing.access_level = access_level
            existing.granted_by = granted_by
            db.add(existing)
            await db.flush()
            return existing

        grant = FilePermission(
            document_id=document_id,
            principal_id=principal_id,
            principal_type=principal_type,
            access_level=access_level,
            granted_by=granted_by,
        )
        db.add(grant)
        await db.flush()
        return grant

    @staticmethod
    async def revoke(
        db: AsyncSession,
        document_id: UUID,
        principal_id: UUID,
        principal_type: str = PrincipalType.USER.value,
    ) -> None:
        grant = await FilePermissionRepository.get(db, document_id, principal_id, principal_type)
        if grant is not None:
            await db.delete(grant)
            await db.flush()
