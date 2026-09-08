import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import settings
from app.core.logging import get_logger
from app.db.postgres import dispose_engine, get_engine
from app.db.session import get_session_factory
from app.events.processing_status_consumer import run_processing_status_consumer
from app.events.producer import start_producer, stop_producer
from app.services.av_scanner import get_scanner
from app.services.quarantine_service import run_quarantine_consumer
from app.storage.bucket_bootstrap import bootstrap_buckets
from app.storage.minio_client import get_minio_client
from app.workers.outbox_worker import run_outbox_worker

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup/shutdown wiring for the File Service.

    Later phases add here (in order, per docs/IMPLEMENTATION_PLAN.md):
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

    # Phase 6: the quarantine/AV-scan consumer (docs/LLD.md §4.5/§6.6). Only
    # meaningful when AV_SCAN_ENABLED — with it off, uploads never land in
    # the quarantine bucket in the first place (upload_service._target_bucket),
    # so there would be nothing for this consumer to ever observe.
    quarantine_consumer_task: asyncio.Task | None = None
    quarantine_consumer_stop_event: asyncio.Event | None = None
    if settings.AV_SCAN_ENABLED:
        scanner = get_scanner()
        quarantine_consumer_stop_event = asyncio.Event()
        quarantine_consumer_task = asyncio.create_task(
            run_quarantine_consumer(minio_client, scanner, get_session_factory(), quarantine_consumer_stop_event)
        )
    app.state.quarantine_consumer_task = quarantine_consumer_task
    app.state.quarantine_consumer_stop_event = quarantine_consumer_stop_event

    # Phase 7: the processing-status consumer (docs/LLD.md §3.6/§6.7) — the
    # only code path allowed to drive UPLOADED -> PROCESSING and
    # PROCESSING -> READY. Always started, unlike the quarantine consumer:
    # it reacts to Retrieval Service's own events regardless of whether
    # AV_SCAN_ENABLED, since file.uploaded (and therefore Retrieval
    # Service's processing) happens either way (docs/LLD.md §4.1/§4.5).
    processing_status_consumer_stop_event = asyncio.Event()
    processing_status_consumer_task = asyncio.create_task(
        run_processing_status_consumer(get_session_factory(), processing_status_consumer_stop_event)
    )
    app.state.processing_status_consumer_task = processing_status_consumer_task
    app.state.processing_status_consumer_stop_event = processing_status_consumer_stop_event

    logger.info("file_service.startup.complete")
    try:
        yield
    finally:
        logger.info("file_service.shutdown.begin")
        outbox_worker_stop_event.set()
        await outbox_worker_task
        if quarantine_consumer_task is not None and quarantine_consumer_stop_event is not None:
            quarantine_consumer_stop_event.set()
            await quarantine_consumer_task
        processing_status_consumer_stop_event.set()
        await processing_status_consumer_task
        await stop_producer()
        await dispose_engine()
        logger.info("file_service.shutdown.complete")
