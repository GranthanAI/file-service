from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxEvent


class OutboxRepository:
    @staticmethod
    async def insert(db: AsyncSession, *, aggregate_id: UUID, event_type: str, payload: dict[str, Any]) -> OutboxEvent:
        event = OutboxEvent(aggregate_id=aggregate_id, event_type=event_type, payload=payload, processed=False)
        db.add(event)
        await db.flush()
        return event

    @staticmethod
    async def fetch_unprocessed_for_update(db: AsyncSession, limit: int = 100) -> list[OutboxEvent]:
        """`FOR UPDATE SKIP LOCKED` so multiple File Service pods can run the
        outbox worker concurrently without double-publishing the same row
        (docs/LLD.md §4.7) — this alone does NOT make publishing exactly-once;
        see docs/LLD.md §3.7 for why consumers must still dedupe on event_id."""
        result = await db.execute(
            select(OutboxEvent)
            .where(OutboxEvent.processed.is_(False))
            .order_by(OutboxEvent.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.scalars().all())

    @staticmethod
    async def mark_processed(db: AsyncSession, event: OutboxEvent) -> None:
        event.processed = True
        db.add(event)
        await db.flush()
