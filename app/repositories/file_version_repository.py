from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.file_version import FileVersion


class FileVersionRepository:
    @staticmethod
    async def insert(
        db: AsyncSession,
        *,
        document_id: UUID,
        version: int,
        storage_key: str,
        etag: str,
        size_bytes: int,
        checksum_sha256: str | None = None,
    ) -> FileVersion:
        version_obj = FileVersion(
            document_id=document_id,
            version=version,
            storage_key=storage_key,
            etag=etag,
            checksum_sha256=checksum_sha256,
            size_bytes=size_bytes,
        )
        db.add(version_obj)
        await db.flush()
        return version_obj

    @staticmethod
    async def list_for_document(db: AsyncSession, document_id: UUID) -> list[FileVersion]:
        result = await db.execute(
            select(FileVersion).where(FileVersion.document_id == document_id).order_by(FileVersion.version.asc())
        )
        return list(result.scalars().all())
