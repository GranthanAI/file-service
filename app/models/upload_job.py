import uuid
from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, String, func, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.enums import UploadJobStatus
from app.db.base import Base


class UploadJob(Base):
    """See docs/LLD.md §2.2. Tracks an in-flight presigned-upload session so
    /complete can validate against the exact terms the URL was issued under."""

    __tablename__ = "upload_jobs"
    __table_args__ = (
        Index(
            "idx_upload_jobs_pending_expiry",
            "expires_at",
            postgresql_where=text("status = 'PENDING'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("files.document_id", ondelete="CASCADE"), nullable=False
    )
    presign_method: Mapped[str] = mapped_column(String(10), nullable=False)
    expected_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    expected_content_type: Mapped[str] = mapped_column(String(150), nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=UploadJobStatus.PENDING.value, server_default=UploadJobStatus.PENDING.value
    )
