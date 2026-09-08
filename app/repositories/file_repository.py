from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import FileStatus
from app.models.file import File
from app.services import lifecycle_service


class FileRepository:
    @staticmethod
    async def insert(
        db: AsyncSession,
        *,
        document_id: UUID,
        conversation_id: UUID | None,
        owner_id: UUID,
        filename: str,
        content_type: str,
        size_bytes: int,
        storage_bucket: str,
        storage_key: str,
    ) -> File:
        file_obj = File(
            document_id=document_id,
            conversation_id=conversation_id,
            owner_id=owner_id,
            filename=filename,
            content_type=content_type,
            size_bytes=size_bytes,
            storage_bucket=storage_bucket,
            storage_key=storage_key,
            status=FileStatus.UPLOADING,
        )
        db.add(file_obj)
        await db.flush()
        return file_obj

    @staticmethod
    async def get(db: AsyncSession, document_id: UUID) -> File | None:
        result = await db.execute(select(File).where(File.document_id == document_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_for_update(db: AsyncSession, document_id: UUID) -> File | None:
        result = await db.execute(select(File).where(File.document_id == document_id).with_for_update())
        return result.scalar_one_or_none()

    @staticmethod
    async def get_by_bucket_and_key(db: AsyncSession, storage_bucket: str, storage_key: str) -> File | None:
        """Looks up the `files` row an inbound MinIO ObjectCreated event
        refers to (docs/LLD.md §4.5/§6.6) — the event carries a bucket/key,
        not a document_id, and `uq_files_bucket_key` (docs/LLD.md §2.1)
        guarantees this is unambiguous."""
        result = await db.execute(
            select(File).where(File.storage_bucket == storage_bucket, File.storage_key == storage_key)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def update_status(db: AsyncSession, file_obj: File, target: FileStatus, **fields) -> File:
        """Validates the edge via lifecycle_service.transition() before
        writing — this is the ONLY place files.status should be assigned
        (docs/LLD.md §4.2); no other repository/service should set
        file_obj.status directly."""
        lifecycle_service.transition(file_obj.status, target)
        file_obj.status = target
        for field, value in fields.items():
            setattr(file_obj, field, value)
        db.add(file_obj)
        await db.flush()
        return file_obj

    @staticmethod
    async def set_pending_version(
        db: AsyncSession, file_obj: File, *, storage_key: str, storage_bucket: str, current_version: int
    ) -> File:
        """docs/HLD.md §16 / docs/LLD.md §2.3 — repoints `files.storage_key`/
        `storage_bucket`/`current_version` at a prospective new version
        BEFORE any bytes have been verified, mirroring exactly how `insert`
        already does this for a brand-new v1 row (its `storage_key` also
        points at an object that doesn't exist yet, from INSERT until
        `/complete` verifies it) — the same accepted transient-inconsistency
        window, just for version N+1 instead of version 1. Deliberately does
        NOT go through `update_status` — no `files.status` change happens
        here, only the version-location pointer, so lifecycle_service.transition
        has nothing to validate."""
        file_obj.storage_key = storage_key
        file_obj.storage_bucket = storage_bucket
        file_obj.current_version = current_version
        db.add(file_obj)
        await db.flush()
        return file_obj

    @staticmethod
    async def increment_retry_count(db: AsyncSession, file_obj: File) -> File:
        file_obj.retry_count += 1
        db.add(file_obj)
        await db.flush()
        return file_obj

    @staticmethod
    async def list_paginated(
        db: AsyncSession,
        *,
        owner_id: UUID | None = None,
        conversation_id: UUID | None = None,
        status: FileStatus | None = None,
        cursor: UUID | None = None,
        limit: int = 20,
    ) -> list[File]:
        """Keyset pagination on document_id (UUIDv7 — see docs/LLD.md §2.1):
        `WHERE document_id > :cursor ORDER BY document_id LIMIT :limit`,
        using the primary key's own B-tree index."""
        query = select(File)

        if status is not None:
            query = query.where(File.status == status)
        else:
            query = query.where(File.status != FileStatus.DELETED)

        if owner_id is not None:
            query = query.where(File.owner_id == owner_id)
        if conversation_id is not None:
            query = query.where(File.conversation_id == conversation_id)
        if cursor is not None:
            query = query.where(File.document_id > cursor)

        query = query.order_by(File.document_id.asc()).limit(limit)

        result = await db.execute(query)
        return list(result.scalars().all())
