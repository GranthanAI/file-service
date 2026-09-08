"""docs/LLD.md §4.6 — operational safety net for abandoned uploads and
orphaned MinIO objects.

Explicitly out of scope, per docs/LLD.md §4.6's own note: this worker never
issues a `ListObjectsV2`/full-bucket `LIST` against MinIO. Everything it
needs — expired presign sessions, abandoned `files` rows, orphaned objects —
is derivable from `upload_jobs` expiry, a `created_at` timeout on `files`,
and the MinIO event stream (bounded by upload rate, not bucket size).
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.enums import FileStatus
from app.core.logging import get_logger
from app.core.metrics import reconciliation_orphans_total
from app.repositories.file_repository import FileRepository
from app.repositories.upload_job_repository import UploadJobRepository
from app.utils.minio_events import parse_object_created_event

logger = get_logger(__name__)

# docs/LLD.md §4.6 step 2: "created_at < now() - 1 hour" — a fixed constant,
# not a setting, since the LLD states this timeout as a specific number
# rather than a tunable, unlike RECONCILIATION_INTERVAL_SECONDS/
# RECONCILIATION_ORPHAN_GRACE_SECONDS which it leaves as policy choices.
ABANDONED_UPLOAD_TIMEOUT = timedelta(hours=1)

ABANDONED_UPLOAD_ERROR_CODE = "ABANDONED_UPLOAD"


@dataclass(frozen=True)
class ReconciliationSummary:
    expired_upload_jobs: int
    abandoned_files_failed: int


async def run_sweep(db: AsyncSession) -> ReconciliationSummary:
    """docs/LLD.md §4.6 steps 1-2 — the DB-only half of one sweep, fully
    testable without Kafka/MinIO (steps 1-2 need neither). Step 3 (the
    MinIO-notification backlog cross-check) is driven separately by
    `_check_orphan_backlog`, which needs a live Kafka consumer — see
    `run_reconciliation_worker`.
    """
    expired_count = await UploadJobRepository.expire_stale(db)

    cutoff = datetime.now(timezone.utc) - ABANDONED_UPLOAD_TIMEOUT
    abandoned = await FileRepository.list_abandoned_uploading(db, cutoff)
    for file_obj in abandoned:
        await FileRepository.update_status(db, file_obj, FileStatus.FAILED, last_error_code=ABANDONED_UPLOAD_ERROR_CODE)

    await db.commit()

    if expired_count or abandoned:
        logger.info(
            "reconciliation.sweep", expired_upload_jobs=expired_count, abandoned_files_failed=len(abandoned)
        )

    return ReconciliationSummary(expired_upload_jobs=expired_count, abandoned_files_failed=len(abandoned))


async def _check_orphan_backlog(db: AsyncSession, consumer: AIOKafkaConsumer) -> int:
    """docs/LLD.md §4.6 step 3 / docs/HLD.md §12 — drains whatever is
    currently sitting on the MinIO ObjectCreated backlog in one bounded poll
    (never a full-bucket LIST) and cross-checks each event's (bucket, key)
    against `files`: no matching row, or a row still stuck in UPLOADING,
    means the object was never (or not yet) properly claimed.

    An event younger than `RECONCILIATION_ORPHAN_GRACE_SECONDS` is left
    uncommitted (offset not advanced past it) so it's redelivered and
    re-evaluated on a later sweep, once a client has had a fair chance to
    actually call /complete — `enable_auto_commit=False` on the consumer is
    what makes this possible; auto-commit would advance past a too-fresh
    message and lose the chance to ever re-check it.
    """
    orphan_count = 0
    now = datetime.now(timezone.utc)
    grace = timedelta(seconds=settings.RECONCILIATION_ORPHAN_GRACE_SECONDS)

    result = await consumer.getmany(
        timeout_ms=settings.RECONCILIATION_POLL_TIMEOUT_MS, max_records=settings.RECONCILIATION_MAX_RECORDS
    )
    for topic_partition, records in result.items():
        commit_offset: int | None = None
        for record in records:
            record_time = datetime.fromtimestamp(record.timestamp / 1000, tz=timezone.utc)
            if now - record_time < grace:
                # Too fresh — stop at this record, don't commit past it, so
                # it (and everything after it in this batch) is redelivered
                # on a later sweep once it's actually old enough to judge.
                break

            try:
                message = json.loads(record.value)
            except (json.JSONDecodeError, TypeError):
                logger.exception("reconciliation.bad_message")
                commit_offset = record.offset + 1
                continue

            for bucket, key in parse_object_created_event(message):
                file = await FileRepository.get_by_bucket_and_key(db, bucket, key)
                if file is None or file.status == FileStatus.UPLOADING:
                    orphan_count += 1
                    reconciliation_orphans_total.inc()
                    logger.warning("reconciliation.orphan_detected", bucket=bucket, key=key)
            commit_offset = record.offset + 1

        if commit_offset is not None:
            await consumer.commit({topic_partition: commit_offset})

    return orphan_count


async def run_reconciliation_worker(
    session_factory: async_sessionmaker[AsyncSession], stop_event: asyncio.Event
) -> None:
    """Background task (started from lifespan.py) — periodic sweeps every
    `RECONCILIATION_INTERVAL_SECONDS`, each combining the DB-only checks
    (`run_sweep`) with one bounded drain of the MinIO notification backlog
    (`_check_orphan_backlog`). One consumer instance is created and reused
    for the worker's whole lifetime (its committed offsets are what makes
    consecutive sweeps not re-evaluate the same already-old events)."""
    consumer = AIOKafkaConsumer(
        settings.MINIO_OBJECT_EVENTS_TOPIC,
        bootstrap_servers=settings.KAFKA_BROKERS,
        group_id=settings.RECONCILIATION_CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    logger.info("reconciliation_worker.started")
    try:
        while not stop_event.is_set():
            try:
                async with session_factory() as db:
                    summary = await run_sweep(db)
                    orphans = await _check_orphan_backlog(db, consumer)
                    if orphans:
                        logger.warning(
                            "reconciliation.sweep_complete",
                            expired_upload_jobs=summary.expired_upload_jobs,
                            abandoned_files_failed=summary.abandoned_files_failed,
                            orphans_flagged=orphans,
                        )
            except Exception:
                logger.exception("reconciliation_worker.sweep_failed")

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=settings.RECONCILIATION_INTERVAL_SECONDS)
            except TimeoutError:
                pass
    finally:
        await consumer.stop()
        logger.info("reconciliation_worker.stopped")
