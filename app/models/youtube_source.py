from datetime import datetime
from uuid import UUID

from sqlalchemy import Boolean, CheckConstraint, DateTime, Float, ForeignKey, Index, Integer, String, text, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

# Extraction lifecycle (the *indexing* lifecycle stays on files.status: UPLOADED -> PROCESSING -> READY | FAILED).
EXTRACTION_STATUSES = ("PENDING", "FETCHING", "FETCHED", "FAILED_RETRYABLE", "FAILED_PERMANENT")


class YoutubeSource(Base):
    """A YouTube video attached as context. It *is* a File Service document (`document_id` = files.document_id), so it
    rides the existing path unchanged: `file_ids` on messages, branch inheritance, permission checks, readiness gating,
    retrieval indexing, deletion. This table adds the video identity and the transcript-extraction lifecycle.

    One live row per (owner, video): repeated or concurrent attachment returns the same document, so a child
    conversation never duplicates a transcript. A removed source keeps its row (`removed_at`) but stops blocking re-attachment.
    The canonical transcript lives in object storage (JSON); chunks/embeddings are derived and rebuildable from it.
    """

    __tablename__ = "youtube_sources"
    __table_args__ = (
        CheckConstraint(
            "extraction_status IN ('PENDING','FETCHING','FETCHED','FAILED_RETRYABLE','FAILED_PERMANENT')",
            name="ck_youtube_sources_status",
        ),
        CheckConstraint("char_length(video_id) = 11", name="ck_youtube_sources_video_id_len"),
        Index("uq_youtube_sources_owner_video_live", "owner_id", "video_id", unique=True, postgresql_where=text("removed_at IS NULL")),
        Index("idx_youtube_sources_owner_created", "owner_id", "created_at"),
    )

    document_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("files.document_id"), primary_key=True)
    owner_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    video_id: Mapped[str] = mapped_column(String(11), nullable=False)
    canonical_url: Mapped[str] = mapped_column(String(100), nullable=False)
    title: Mapped[str | None] = mapped_column(String(300), nullable=True)
    language: Mapped[str | None] = mapped_column(String(35), nullable=True)
    language_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    is_generated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    provider: Mapped[str | None] = mapped_column(String(60), nullable=True)
    transcript_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    transcript_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    segment_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    extraction_status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING", server_default="PENDING")
    error_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
