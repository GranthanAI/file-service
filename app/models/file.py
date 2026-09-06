from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Enum as SQLEnum, Index, Integer, String, Text, func, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.enums import FileStatus
from app.db.base import Base


class File(Base):
    """See docs/LLD.md §2.1 for the full schema rationale."""

    __tablename__ = "files"
    __table_args__ = (
        CheckConstraint("size_bytes >= 0", name="ck_files_size_bytes_non_negative"),
        Index("idx_files_owner_id", "owner_id"),
        Index("idx_files_conversation_id", "conversation_id"),
        Index(
            "idx_files_status",
            "status",
            postgresql_where=text("status <> 'DELETED'"),
        ),
        Index("uq_files_bucket_key", "storage_bucket", "storage_key", unique=True),
    )

    # App-generated UUIDv7 (see app/utils/id_generator.py), not a DB default —
    # the File Service owns document_id generation (docs/HLD.md §8).
    document_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    conversation_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    owner_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    content_type: Mapped[str] = mapped_column(String(150), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    storage_bucket: Mapped[str] = mapped_column(String(63), nullable=False)
    storage_key: Mapped[str] = mapped_column(Text, nullable=False)
    etag: Mapped[str | None] = mapped_column(Text, nullable=True)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[FileStatus] = mapped_column(
        SQLEnum(FileStatus, name="file_status"),
        nullable=False,
        default=FileStatus.UPLOADING,
        server_default=FileStatus.UPLOADING.value,
    )
    current_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
