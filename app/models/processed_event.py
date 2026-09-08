from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ProcessedEventId(Base):
    """docs/LLD.md §3.7 / Phase 7 task 2: dedupe record for events consumed
    from Kafka's at-least-once delivery — every consumer must check this
    before acting on a message and record it atomically once it has.

    Keyed by (event_id, consumer) rather than event_id alone so a future
    second consumer of the same topic (docs/LLD.md §3.7: "a hard requirement
    for any future consumer") tracks its own dedupe state independently,
    without the two consumers racing to insert the same primary key for
    what is, from Kafka's perspective, one delivery each.
    """

    __tablename__ = "processed_event_ids"

    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    consumer: Mapped[str] = mapped_column(String(100), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
