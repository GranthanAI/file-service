import uuid
from datetime import datetime
from uuid import UUID

from sqlalchemy import Boolean, DateTime, Index, String, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OutboxEvent(Base):
    """See docs/LLD.md §2.5 (Transactional Outbox Pattern) and §3.7 (Kafka
    is at-least-once — consumers must dedupe on event_id, not rely on this
    table alone for exactly-once delivery)."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        Index(
            "idx_outbox_unprocessed",
            "created_at",
            postgresql_where=text("processed = false"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    aggregate_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    processed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
