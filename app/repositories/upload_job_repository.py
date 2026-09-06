from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import UploadJobStatus
from app.models.upload_job import UploadJob


class UploadJobRepository:
    @staticmethod
    async def insert(
        db: AsyncSession,
        *,
        document_id: UUID,
        presign_method: str,
        expected_size: int,
        expected_content_type: str,
        expires_at: datetime,
    ) -> UploadJob:
        job = UploadJob(
            document_id=document_id,
            presign_method=presign_method,
            expected_size=expected_size,
            expected_content_type=expected_content_type,
            expires_at=expires_at,
            status=UploadJobStatus.PENDING.value,
        )
        db.add(job)
        await db.flush()
        return job

    @staticmethod
    async def get_pending(db: AsyncSession, document_id: UUID) -> UploadJob | None:
        """Most recent PENDING attempt for a document — a document_id can
        accumulate multiple upload attempts (docs/LLD.md §2.2)."""
        result = await db.execute(
            select(UploadJob)
            .where(UploadJob.document_id == document_id, UploadJob.status == UploadJobStatus.PENDING.value)
            .order_by(UploadJob.issued_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def mark_completed(db: AsyncSession, job: UploadJob) -> UploadJob:
        job.status = UploadJobStatus.COMPLETED.value
        job.completed_at = datetime.now(timezone.utc)
        db.add(job)
        await db.flush()
        return job

    @staticmethod
    async def expire_stale(db: AsyncSession) -> int:
        """Sweeps PENDING jobs past their expires_at into EXPIRED — used by
        the reconciliation worker (docs/LLD.md §4.6), not by request-path
        code. Returns the number of rows updated."""
        result = await db.execute(
            update(UploadJob)
            .where(
                UploadJob.status == UploadJobStatus.PENDING.value,
                UploadJob.expires_at < datetime.now(timezone.utc),
            )
            .values(status=UploadJobStatus.EXPIRED.value)
        )
        await db.flush()
        return result.rowcount or 0
