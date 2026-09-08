import json
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.core.metrics import outbox_publish_lag_seconds
from app.events.file_events import EVENT_MODELS
from app.events.topics import PRODUCED_TOPICS
from app.models.outbox import OutboxEvent
from app.repositories.outbox_repository import OutboxRepository

logger = get_logger(__name__)


def _build_message(row: OutboxEvent) -> bytes:
    """Stamps event_id/event_type/version onto the row's stored payload
    (docs/HLD.md §13) — see file_events.py's module docstring for why those
    three fields are added here rather than duplicated by every service that
    inserts an outbox row. Validates the assembled envelope against the
    Pydantic model registered for this event_type when one exists, so a
    malformed row raises here (and is logged, see publish_batch) instead of
    putting bad JSON on the topic."""
    envelope = {"event_id": str(row.id), "event_type": row.event_type, "version": 1, **row.payload}
    model = EVENT_MODELS.get(row.event_type)
    if model is not None:
        return model.model_validate(envelope).model_dump_json().encode("utf-8")
    return json.dumps(envelope).encode("utf-8")


async def publish_batch(db: AsyncSession, producer: AIOKafkaProducer, *, limit: int | None = None) -> int:
    """docs/LLD.md §4.7 — one poll-and-publish cycle.

    Fetches up to `limit` unprocessed rows via `FOR UPDATE SKIP LOCKED`
    (docs/LLD.md §4.7 — lets multiple File Service pods run this concurrently
    against the same backlog without ever double-publishing the same row),
    publishes each to the Kafka topic named by its event_type, and marks it
    processed only after the broker ACKs (`send_and_wait`). The whole batch
    is committed together at the end, releasing the row locks.

    This function does NOT achieve exactly-once delivery: if the process
    crashes between a given row's Kafka ACK and this function's closing
    commit, that row is still `processed = FALSE` and gets republished on
    the next poll. This is the documented at-least-once behavior (docs/LLD.md
    §3.7), not a bug — every consumer of these topics must dedupe on
    event_id.

    Returns the number of rows published this cycle (0 means the backlog was
    empty).
    """
    rows = await OutboxRepository.fetch_unprocessed_for_update(db, limit=limit if limit is not None else settings.OUTBOX_BATCH_SIZE)
    published = 0
    for row in rows:
        if row.event_type not in PRODUCED_TOPICS:
            # Logged and skipped, not raised: one bad row (e.g. a future
            # event_type this deployed version doesn't know about yet)
            # should never wedge the whole batch — every other row in the
            # backlog still deserves to be published.
            logger.error("outbox.unknown_event_type", event_id=str(row.id), event_type=row.event_type)
            continue

        message = _build_message(row)
        await producer.send_and_wait(row.event_type, message)
        await OutboxRepository.mark_processed(db, row)
        published += 1
        # docs/HLD.md §21 outbox_publish_lag_seconds — time between the row
        # entering the outbox (created_at) and actually reaching Kafka.
        lag = (datetime.now(timezone.utc) - row.created_at).total_seconds()
        outbox_publish_lag_seconds.observe(max(lag, 0))
        logger.info("outbox.published", event_id=str(row.id), event_type=row.event_type)

    await db.commit()
    return published
