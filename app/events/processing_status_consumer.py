"""docs/LLD.md §3.6/§3.7/§6.7 — consumes Retrieval Service's three
processing-status events and is the ONLY code path allowed to drive the
UPLOADED -> PROCESSING and PROCESSING -> READY edges (docs/LLD.md §4.2's
comment on ALLOWED_TRANSITIONS is explicit about this: neither
upload_service.py nor quarantine_service.py ever calls transition() with
those targets).

Dispatch is by Kafka topic, not by a field inside the message body — the
three payload shapes shown in docs/LLD.md §3.6 (`file.processing.started` /
`.completed` / `.failed`) carry no `event_type` field, unlike the events
File Service itself produces (docs/HLD.md §13's `file.uploaded`).
"""

import asyncio
import json
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.enums import FileStatus
from app.core.exceptions import InvalidStateTransitionError
from app.core.logging import get_logger
from app.events.topics import FILE_PROCESSING_COMPLETED, FILE_PROCESSING_FAILED, FILE_PROCESSING_STARTED
from app.repositories.file_repository import FileRepository
from app.repositories.processed_event_repository import ProcessedEventRepository

logger = get_logger(__name__)

CONSUMER_NAME = "processing_status_consumer"

_TARGET_STATUS_BY_TOPIC = {
    FILE_PROCESSING_STARTED: FileStatus.PROCESSING,
    FILE_PROCESSING_COMPLETED: FileStatus.READY,
    FILE_PROCESSING_FAILED: FileStatus.FAILED,
}


async def process_message(db: AsyncSession, topic: str, payload: dict) -> None:
    """docs/LLD.md §3.6's handler table for one already-deserialized message.

    Every branch below commits and returns rather than raising, EXCEPT an
    illegal state transition (docs/LLD.md §4.2) — that's a genuine signal
    something is wrong (e.g. `file.processing.completed` for a file that was
    never started) and is logged, not silently swallowed, though it still
    doesn't kill the consumer loop (see run_processing_status_consumer).
    """
    event_id = UUID(payload["event_id"])
    is_new = await ProcessedEventRepository.mark_if_new(db, event_id, CONSUMER_NAME)
    if not is_new:
        # docs/LLD.md §3.7: at-least-once delivery means a duplicate
        # event_id is expected, not an error — a no-op, exactly like
        # lifecycle_service.transition's own current==target no-op.
        logger.info("processing_status_consumer.duplicate", event_id=str(event_id), topic=topic)
        await db.commit()
        return

    document_id = UUID(payload["document_id"])
    file = await FileRepository.get(db, document_id)
    if file is None:
        logger.warning("processing_status_consumer.unknown_document", document_id=str(document_id), topic=topic)
        await db.commit()
        return

    target_status = _TARGET_STATUS_BY_TOPIC.get(topic)
    if target_status is None:
        logger.warning("processing_status_consumer.unknown_topic", topic=topic)
        await db.commit()
        return

    extra_fields = {}
    if topic == FILE_PROCESSING_FAILED:
        extra_fields = {
            "last_error_code": payload.get("error_code"),
            "last_error_message": payload.get("error_message"),
        }

    try:
        await FileRepository.update_status(db, file, target_status, **extra_fields)
    except InvalidStateTransitionError:
        logger.exception(
            "processing_status_consumer.illegal_transition",
            document_id=str(document_id),
            current=file.status.value,
            target=target_status.value,
            topic=topic,
        )
        await db.rollback()
        return

    await db.commit()
    logger.info(
        "processing_status_consumer.transitioned", document_id=str(document_id), status=target_status.value, topic=topic
    )


async def run_processing_status_consumer(
    session_factory: async_sessionmaker[AsyncSession], stop_event: asyncio.Event
) -> None:
    """Background consumer task (started from lifespan.py, mirrors
    outbox_worker.py / quarantine_service.run_quarantine_consumer's shape),
    subscribed to all three processing-status topics at once."""
    consumer = AIOKafkaConsumer(
        FILE_PROCESSING_STARTED,
        FILE_PROCESSING_COMPLETED,
        FILE_PROCESSING_FAILED,
        bootstrap_servers=settings.KAFKA_BROKERS,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    logger.info("processing_status_consumer.started")
    try:
        while not stop_event.is_set():
            try:
                result = await consumer.getmany(timeout_ms=1000, max_records=50)
            except Exception:
                logger.exception("processing_status_consumer.poll_failed")
                continue

            for topic_partition, records in result.items():
                for record in records:
                    try:
                        payload = json.loads(record.value)
                    except (json.JSONDecodeError, TypeError):
                        logger.exception("processing_status_consumer.bad_message")
                        continue

                    try:
                        async with session_factory() as db:
                            await process_message(db, topic_partition.topic, payload)
                    except Exception:
                        # One bad event must not kill the consumer loop —
                        # matches quarantine_service's per-event isolation.
                        logger.exception("processing_status_consumer.event_failed", topic=topic_partition.topic)
    finally:
        await consumer.stop()
        logger.info("processing_status_consumer.stopped")
