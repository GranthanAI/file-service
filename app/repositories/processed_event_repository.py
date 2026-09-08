from uuid import UUID

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.processed_event import ProcessedEventId


class ProcessedEventRepository:
    @staticmethod
    async def mark_if_new(db: AsyncSession, event_id: UUID, consumer: str) -> bool:
        """Atomically checks-and-records whether (event_id, consumer) has
        been seen before (docs/LLD.md §3.7 dedupe requirement). `INSERT ...
        ON CONFLICT DO NOTHING` rather than a separate SELECT-then-INSERT —
        the single statement is what makes this safe under concurrent
        delivery/retries without a race between the check and the record.

        Returns True if this is the first time (event_id, consumer) has been
        seen — the caller should process the event. Returns False if it was
        already recorded — the caller should skip it as a duplicate.
        """
        stmt = (
            insert(ProcessedEventId)
            .values(event_id=event_id, consumer=consumer)
            .on_conflict_do_nothing(index_elements=["event_id", "consumer"])
            .returning(ProcessedEventId.event_id)
        )
        result = await db.execute(stmt)
        await db.flush()
        return result.first() is not None
