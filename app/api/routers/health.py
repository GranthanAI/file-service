import asyncio

from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import PlainTextResponse
from minio import Minio
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.core.config import settings
from app.core.logging import get_logger
from app.storage.minio_client import get_minio_client

logger = get_logger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    """Process-alive check only — no dependency calls.

    Deliberately never checks DB/MinIO/Kafka: a slow dependency must not
    make this fail, since that would cause restart-loops that fix nothing.
    See docs/LLD.md §4.8 for the full liveness/readiness split.
    """
    return {"status": "alive"}


async def _check_database(db: AsyncSession) -> bool:
    try:
        await asyncio.wait_for(db.execute(text("SELECT 1")), timeout=settings.HEALTH_CHECK_TIMEOUT_SECONDS)
        return True
    except Exception:
        logger.exception("health.ready.database_check_failed")
        return False


async def _check_minio(minio_client: Minio) -> bool:
    try:
        # bucket_exists is a synchronous (blocking) HTTP call — offloaded to
        # a thread so a slow/hung MinIO can't block the event loop, and
        # wrapped in the same timeout as the DB check.
        await asyncio.wait_for(
            asyncio.to_thread(minio_client.bucket_exists, settings.BUCKET_FILES),
            timeout=settings.HEALTH_CHECK_TIMEOUT_SECONDS,
        )
        return True
    except Exception:
        logger.exception("health.ready.minio_check_failed")
        return False


@router.get("/health/ready")
async def readiness(
    response: Response,
    db: AsyncSession = Depends(get_db),
    minio_client: Minio = Depends(get_minio_client),
) -> dict[str, object]:
    """docs/LLD.md §4.8 — 200 only if PostgreSQL AND MinIO both respond
    within HEALTH_CHECK_TIMEOUT_SECONDS. Kafka is deliberately excluded: a
    Kafka outage never blocks upload initiate/complete (those only write to
    outbox_events inside the same PostgreSQL transaction — docs/LLD.md
    §4.8's own rationale, mirrored in docs/HLD.md §13/§20) — wiring Kafka
    into this probe would take every pod NotReady for a dependency the
    architecture was specifically designed to tolerate being briefly down.
    """
    database_ok = await _check_database(db)
    minio_ok = await _check_minio(minio_client)

    if not (database_ok and minio_ok):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not_ready", "database": database_ok, "minio": minio_ok}

    return {"status": "ready", "database": True, "minio": True}


@router.get("/metrics")
async def metrics() -> PlainTextResponse:
    """docs/HLD.md §21 — Prometheus scrape endpoint for the metric set
    defined in app/core/metrics.py."""
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
