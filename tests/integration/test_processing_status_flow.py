"""Phase 7 exit-criteria tests (docs/IMPLEMENTATION_PLAN.md): the
processing-status contract (docs/LLD.md §3.6/§6.7) driven end-to-end through
a real Kafka broker and a real, running `run_processing_status_consumer`
background task — a fake Retrieval Service is just this test file publishing
the three event types directly to the real topics.
"""

import asyncio
import json
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from unittest.mock import patch

from aiokafka import AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.kafka import KafkaContainer

from app.core.config import settings
from app.core.enums import FileStatus
from app.events.processing_status_consumer import run_processing_status_consumer
from app.events.topics import FILE_PROCESSING_COMPLETED, FILE_PROCESSING_FAILED, FILE_PROCESSING_STARTED
from app.models.file import File
from app.repositories.file_repository import FileRepository
from app.utils.id_generator import new_uuid7


async def _insert_uploaded_file(session_factory: async_sessionmaker[AsyncSession]) -> uuid.UUID:
    document_id = new_uuid7()
    owner_id = uuid.uuid4()
    async with session_factory() as db:
        file_obj = await FileRepository.insert(
            db,
            document_id=document_id,
            conversation_id=None,
            owner_id=owner_id,
            filename="physics.pdf",
            content_type="application/pdf",
            size_bytes=1024,
            storage_bucket="graphgpt-files",
            storage_key=f"{owner_id}/{document_id}/v1/original.pdf",
        )
        # UPLOADED, not the freshly-inserted UPLOADING default — this
        # simulates the file already having gone through /complete (and,
        # when AV_SCAN_ENABLED, quarantine promotion), which is the only
        # state file.processing.started legitimately fires from.
        await FileRepository.update_status(db, file_obj, FileStatus.UPLOADED, etag="fake-etag")
        await db.commit()
    return document_id


async def _wait_for_status(
    session_factory: async_sessionmaker[AsyncSession], document_id: uuid.UUID, expected: FileStatus, *, timeout_s: float = 20
) -> File:
    deadline = time.monotonic() + timeout_s
    file = None
    while time.monotonic() < deadline:
        async with session_factory() as db:
            file = await FileRepository.get(db, document_id)
            if file is not None and file.status == expected:
                return file
        await asyncio.sleep(0.2)
    raise AssertionError(f"document {document_id} never reached status {expected}; last seen: {file.status if file else None}")


@asynccontextmanager
async def _running_consumer(
    session_factory: async_sessionmaker[AsyncSession], kafka_container: KafkaContainer
) -> AsyncGenerator[None, None]:
    """Runs the real `run_processing_status_consumer` background task for
    the duration of the `with` block, pointed at the ephemeral test Kafka
    broker rather than settings.KAFKA_BROKERS' default (localhost:9092) —
    the consumer reads that setting internally when it constructs its own
    AIOKafkaConsumer, so it has to be patched for the task's lifetime, the
    same way app_client's dependency_overrides redirect the DB/MinIO."""
    with patch.object(settings, "KAFKA_BROKERS", kafka_container.get_bootstrap_server()):
        stop_event = asyncio.Event()
        task = asyncio.create_task(run_processing_status_consumer(session_factory, stop_event))
        try:
            yield
        finally:
            stop_event.set()
            await asyncio.wait_for(task, timeout=10)


async def test_started_then_completed_drives_uploaded_to_ready(
    migrated_database_url: str, kafka_container: KafkaContainer, kafka_producer: AIOKafkaProducer
) -> None:
    engine = create_async_engine(migrated_database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    document_id = await _insert_uploaded_file(session_factory)

    async with _running_consumer(session_factory, kafka_container):
        await kafka_producer.send_and_wait(
            FILE_PROCESSING_STARTED,
            json.dumps({"event_id": str(uuid.uuid4()), "document_id": str(document_id), "timestamp": "2026-09-09T00:00:00Z"}).encode(),
        )
        await _wait_for_status(session_factory, document_id, FileStatus.PROCESSING)

        await kafka_producer.send_and_wait(
            FILE_PROCESSING_COMPLETED,
            json.dumps(
                {"event_id": str(uuid.uuid4()), "document_id": str(document_id), "chunk_count": 42, "timestamp": "2026-09-09T00:00:00Z"}
            ).encode(),
        )
        await _wait_for_status(session_factory, document_id, FileStatus.READY)

    await engine.dispose()


async def test_failed_sets_error_fields_and_status(
    migrated_database_url: str, kafka_container: KafkaContainer, kafka_producer: AIOKafkaProducer
) -> None:
    engine = create_async_engine(migrated_database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    document_id = await _insert_uploaded_file(session_factory)

    async with _running_consumer(session_factory, kafka_container):
        await kafka_producer.send_and_wait(
            FILE_PROCESSING_STARTED,
            json.dumps({"event_id": str(uuid.uuid4()), "document_id": str(document_id), "timestamp": "2026-09-09T00:00:00Z"}).encode(),
        )
        await _wait_for_status(session_factory, document_id, FileStatus.PROCESSING)

        await kafka_producer.send_and_wait(
            FILE_PROCESSING_FAILED,
            json.dumps(
                {
                    "event_id": str(uuid.uuid4()),
                    "document_id": str(document_id),
                    "error_code": "PARSE_ERROR",
                    "error_message": "Unsupported PDF encoding",
                    "timestamp": "2026-09-09T00:00:00Z",
                }
            ).encode(),
        )
        file = await _wait_for_status(session_factory, document_id, FileStatus.FAILED)
        assert file.last_error_code == "PARSE_ERROR"
        assert file.last_error_message == "Unsupported PDF encoding"

    await engine.dispose()


async def test_duplicate_completed_event_id_is_a_noop(
    migrated_database_url: str, kafka_container: KafkaContainer, kafka_producer: AIOKafkaProducer
) -> None:
    """docs/LLD.md §3.7 idempotency exit criterion, over real Kafka: the
    same event_id published twice must not double-process (no error, no
    illegal-transition attempt on the second delivery)."""
    engine = create_async_engine(migrated_database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    document_id = await _insert_uploaded_file(session_factory)

    async with _running_consumer(session_factory, kafka_container):
        await kafka_producer.send_and_wait(
            FILE_PROCESSING_STARTED,
            json.dumps({"event_id": str(uuid.uuid4()), "document_id": str(document_id), "timestamp": "2026-09-09T00:00:00Z"}).encode(),
        )
        await _wait_for_status(session_factory, document_id, FileStatus.PROCESSING)

        completed_event_id = str(uuid.uuid4())
        completed_message = json.dumps(
            {"event_id": completed_event_id, "document_id": str(document_id), "chunk_count": 42, "timestamp": "2026-09-09T00:00:00Z"}
        ).encode()
        await kafka_producer.send_and_wait(FILE_PROCESSING_COMPLETED, completed_message)
        await _wait_for_status(session_factory, document_id, FileStatus.READY)

        # Redeliver the exact same event_id — must remain READY, no error.
        await kafka_producer.send_and_wait(FILE_PROCESSING_COMPLETED, completed_message)
        await asyncio.sleep(2)  # give the consumer a chance to (mis)process it

        async with session_factory() as db:
            file = await FileRepository.get(db, document_id)
        assert file.status == FileStatus.READY

    await engine.dispose()
