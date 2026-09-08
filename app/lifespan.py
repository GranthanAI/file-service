import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.logging import get_logger
from app.db.postgres import dispose_engine, get_engine
from app.events.producer import start_producer, stop_producer
from app.storage.bucket_bootstrap import bootstrap_buckets
from app.storage.minio_client import get_minio_client
from app.workers.outbox_worker import run_outbox_worker

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup/shutdown wiring for the File Service.

    Later phases add here (in order, per docs/IMPLEMENTATION_PLAN.md):
      - Phase 6: quarantine_service consumer background task
      - Phase 7: processing_status_consumer background task
      - Phase 9: reconciliation_worker background task
    """
    logger.info("file_service.startup.begin")

    engine = get_engine()
    app.state.db_engine = engine

    minio_client = get_minio_client()
    app.state.minio_client = minio_client
    bootstrap_buckets(minio_client)

    # Phase 5: Kafka producer + the outbox worker that drains outbox_events
    # onto it (docs/LLD.md §4.7). stop_event is how shutdown below tells the
    # worker's poll loop to exit instead of waiting out its next interval.
    producer = await start_producer()
    app.state.kafka_producer = producer
    outbox_worker_stop_event = asyncio.Event()
    outbox_worker_task = asyncio.create_task(run_outbox_worker(producer, outbox_worker_stop_event))
    app.state.outbox_worker_stop_event = outbox_worker_stop_event
    app.state.outbox_worker_task = outbox_worker_task

    logger.info("file_service.startup.complete")
    try:
        yield
    finally:
        logger.info("file_service.shutdown.begin")
        outbox_worker_stop_event.set()
        await outbox_worker_task
        await stop_producer()
        await dispose_engine()
        logger.info("file_service.shutdown.complete")
