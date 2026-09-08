"""docs/LLD.md §4.5/§6.6 — the single authoritative flow from "bytes landed
in the quarantine bucket" to `file.uploaded`. See lifecycle_service.py's
comment for why the clean path lands back on UPLOADED, never PROCESSING —
that edge belongs exclusively to processing_status_consumer.py (Phase 7).
"""

import asyncio
import hashlib
import json
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from minio import Minio
from minio.commonconfig import CopySource
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.enums import FileStatus
from app.core.logging import get_logger
from app.models.file import File
from app.repositories.file_repository import FileRepository
from app.repositories.outbox_repository import OutboxRepository
from app.services.av_scanner import Scanner
from app.utils.minio_events import parse_object_created_event
from app.validation.magic_bytes import sniff_content_type

logger = get_logger(__name__)

AV_SCAN_INFECTED = "AV_SCAN_INFECTED"


def _build_file_uploaded_payload(file: File) -> dict:
    """docs/HLD.md §13 — mirrors upload_service._build_file_uploaded_payload
    exactly (same wire shape, same reasoning for why event_id/event_type/
    version aren't set here — outbox_publisher.py stamps those at publish
    time). The one difference: `storage.bucket` is always BUCKET_FILES here,
    since this function only ever runs after a successful promotion out of
    quarantine."""
    return {
        "document_id": str(file.document_id),
        "conversation_id": str(file.conversation_id) if file.conversation_id else None,
        "owner_id": str(file.owner_id),
        "filename": file.filename,
        "content_type": file.content_type,
        "storage": {
            "provider": "minio",
            "bucket": settings.BUCKET_FILES,
            "key": file.storage_key,
            "etag": file.etag,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _build_file_scan_failed_payload(file: File) -> dict:
    """docs/LLD.md §4.5 — no explicit wire shape given beyond "insert
    outbox_events row: file.scan_failed"; matches FileScanFailedEvent
    (app/events/file_events.py)."""
    return {
        "document_id": str(file.document_id),
        "error_code": AV_SCAN_INFECTED,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def process_object_created(db: AsyncSession, minio_client: Minio, scanner: Scanner, *, bucket: str, key: str) -> None:
    """docs/LLD.md §4.5/§6.6 — handles one MinIO ObjectCreated event for the
    quarantine bucket, end to end.

    Deliberately tolerant of events that aren't ours to act on, since this
    consumes a raw MinIO bucket-notification stream (docs/HLD.md §12), not
    the File Service's own outbox — every branch below is a no-op rather
    than an error for a case outside this function's job:
      - no matching `files` row (a foreign object, or a race with the row
        insert — the reconciliation worker's job to catch, not this one's).
      - the row isn't currently UPLOADED (already scanned, or redelivered
        under Kafka's at-least-once semantics, docs/LLD.md §3.7) — a no-op
        keeps this handler idempotent under duplicate delivery.
    """
    file = await FileRepository.get_by_bucket_and_key(db, bucket, key)
    if file is None:
        logger.warning("quarantine.unknown_object", bucket=bucket, key=key)
        return

    if file.status != FileStatus.UPLOADED:
        logger.info(
            "quarantine.skipped_not_uploaded", document_id=str(file.document_id), status=file.status.value
        )
        return

    # UPLOADED -> SCANNING (docs/LLD.md §4.2) — from here on, the file is
    # correctly reported as "being scanned" to any client polling GET
    # /files/{document_id}/status while this handler runs.
    await FileRepository.update_status(db, file, FileStatus.SCANNING)
    await db.commit()

    # One read, three outputs (docs/LLD.md Phase 6 task 3): magic-byte
    # sniffing, the AV scan, and the SHA-256 checksum all come from this
    # single GetObject body read — the same trust-chain step /complete
    # deliberately defers (docs/LLD.md §3.5) because /complete only calls
    # StatObject, never reading the object body.
    response = minio_client.get_object(bucket, key)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()

    sniffed_content_type = sniff_content_type(data)
    if sniffed_content_type is not None and sniffed_content_type != file.content_type:
        # Logged, not enforced: docs/LLD.md §4.5/§6.6's Clean/Infected
        # branching is driven exclusively by the AV scan verdict — there is
        # no third, magic-byte-mismatch outcome defined in that flow. This
        # is still useful signal for the reconciliation/observability side.
        logger.warning(
            "quarantine.content_type_mismatch",
            document_id=str(file.document_id),
            declared=file.content_type,
            sniffed=sniffed_content_type,
        )

    checksum_sha256 = hashlib.sha256(data).hexdigest()
    verdict = await scanner.scan(data)

    if verdict.clean:
        await _promote_clean_object(db, minio_client, file, bucket=bucket, key=key, checksum_sha256=checksum_sha256)
    else:
        await _reject_infected_object(db, minio_client, file, bucket=bucket, key=key, signature=verdict.signature)


async def _promote_clean_object(
    db: AsyncSession, minio_client: Minio, file: File, *, bucket: str, key: str, checksum_sha256: str
) -> None:
    minio_client.copy_object(settings.BUCKET_FILES, key, CopySource(bucket, key))
    minio_client.remove_object(bucket, key)

    # SCANNING -> UPLOADED, NOT -> PROCESSING (docs/LLD.md §4.2/§4.5's
    # explicit correction) — this is what triggers publishing file.uploaded;
    # crossing into PROCESSING is exclusively processing_status_consumer.py's
    # job (Phase 7), never this function's.
    await FileRepository.update_status(
        db,
        file,
        FileStatus.UPLOADED,
        storage_bucket=settings.BUCKET_FILES,
        checksum_sha256=checksum_sha256,
    )
    await OutboxRepository.insert(
        db,
        aggregate_id=file.document_id,
        event_type="file.uploaded",
        payload=_build_file_uploaded_payload(file),
    )
    await db.commit()
    logger.info("quarantine.promoted", document_id=str(file.document_id))


async def _reject_infected_object(
    db: AsyncSession, minio_client: Minio, file: File, *, bucket: str, key: str, signature: str | None
) -> None:
    minio_client.remove_object(bucket, key)

    # SCANNING -> FAILED (docs/LLD.md §4.2/§4.5) — file.uploaded is NEVER
    # emitted for this document_id; only file.scan_failed is.
    await FileRepository.update_status(db, file, FileStatus.FAILED, last_error_code=AV_SCAN_INFECTED)
    await OutboxRepository.insert(
        db,
        aggregate_id=file.document_id,
        event_type="file.scan_failed",
        payload=_build_file_scan_failed_payload(file),
    )
    await db.commit()
    logger.warning("quarantine.infected", document_id=str(file.document_id), signature=signature)


async def run_quarantine_consumer(
    minio_client: Minio, scanner: Scanner, session_factory: async_sessionmaker[AsyncSession], stop_event: asyncio.Event
) -> None:
    """Background consumer task (started from lifespan.py, mirrors
    outbox_worker.py's shape) for settings.MINIO_OBJECT_EVENTS_TOPIC
    (docs/HLD.md §12) — the MinIO bucket-notification stream for the
    quarantine bucket. Only relevant when settings.AV_SCAN_ENABLED and
    settings.MINIO_NOTIFICATIONS_ENABLED are both true; the caller decides
    whether to start this task at all."""
    consumer = AIOKafkaConsumer(
        settings.MINIO_OBJECT_EVENTS_TOPIC,
        bootstrap_servers=settings.KAFKA_BROKERS,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    logger.info("quarantine_consumer.started", topic=settings.MINIO_OBJECT_EVENTS_TOPIC)
    try:
        while not stop_event.is_set():
            try:
                result = await consumer.getmany(timeout_ms=1000, max_records=50)
            except Exception:
                logger.exception("quarantine_consumer.poll_failed")
                continue

            for records in result.values():
                for record in records:
                    try:
                        message = json.loads(record.value)
                    except (json.JSONDecodeError, TypeError):
                        logger.exception("quarantine_consumer.bad_message")
                        continue

                    for bucket, key in parse_object_created_event(message):
                        try:
                            async with session_factory() as db:
                                await process_object_created(db, minio_client, scanner, bucket=bucket, key=key)
                        except Exception:
                            # One bad event must not kill the consumer loop —
                            # the row (if it exists) stays UPLOADED/SCANNING
                            # and is picked up again on the next redelivery,
                            # or flagged by the reconciliation worker (Phase 9).
                            logger.exception(
                                "quarantine_consumer.event_failed", bucket=bucket, key=key
                            )
    finally:
        await consumer.stop()
        logger.info("quarantine_consumer.stopped")
