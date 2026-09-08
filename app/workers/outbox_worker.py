import asyncio

from aiokafka import AIOKafkaProducer

from app.core.config import settings
from app.core.logging import get_logger
from app.db.session import get_session_factory
from app.events.outbox_publisher import publish_batch

logger = get_logger(__name__)


async def run_outbox_worker(producer: AIOKafkaProducer, stop_event: asyncio.Event) -> None:
    """docs/LLD.md §4.7 — background loop wrapping outbox_publisher.publish_batch,
    started as an asyncio task from lifespan.py and cancelled/awaited via
    `stop_event` on shutdown.

    A fixed-interval poll (every OUTBOX_POLL_INTERVAL_MS, regardless of
    whether the previous poll found anything), not a queue with a blocking
    pop — simple, and matches the polling shape described throughout
    docs/LLD.md §4.7/§6.3 rather than trying to get Postgres LISTEN/NOTIFY or
    logical replication involved for something this low-latency-tolerant.

    Each poll gets its own fresh AsyncSession (never held open across polls)
    so a stuck/leaked connection from one bad poll can't accumulate forever,
    and any exception during a poll is logged and swallowed — one failed
    batch (e.g. a transient DB blip) must not kill the whole background loop,
    since the rows it failed to publish simply remain `processed = FALSE`
    and are retried on the next poll (docs/LLD.md §3.7's at-least-once
    guarantee applies here too).
    """
    session_factory = get_session_factory()
    poll_interval_seconds = settings.OUTBOX_POLL_INTERVAL_MS / 1000
    logger.info("outbox_worker.started", poll_interval_seconds=poll_interval_seconds)

    while not stop_event.is_set():
        try:
            async with session_factory() as db:
                published = await publish_batch(db, producer)
                if published:
                    logger.info("outbox_worker.batch_published", count=published)
        except Exception:
            logger.exception("outbox_worker.batch_failed")

        try:
            # Doubles as the poll-interval sleep and an early-exit signal —
            # a shutdown doesn't have to wait out a full idle interval.
            await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
        except TimeoutError:
            pass

    logger.info("outbox_worker.stopped")
