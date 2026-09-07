from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.logging import get_logger
from app.db.postgres import dispose_engine, get_engine
from app.storage.bucket_bootstrap import bootstrap_buckets
from app.storage.minio_client import get_minio_client

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup/shutdown wiring for the File Service.

    Later phases add here (in order, per docs/IMPLEMENTATION_PLAN.md):
      - Phase 5: Kafka producer start/stop, outbox_worker background task
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

    logger.info("file_service.startup.complete")
    try:
        yield
    finally:
        logger.info("file_service.shutdown.begin")
        await dispose_engine()
        logger.info("file_service.shutdown.complete")
