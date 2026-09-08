"""Phase 5 exit-criteria tests (docs/IMPLEMENTATION_PLAN.md): outbox rows
actually reach Kafka, reliably, with the documented at-least-once semantics
(docs/LLD.md §3.7, §4.7) — against a real, ephemeral Kafka (testcontainers),
not a mocked producer/consumer.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.kafka import KafkaContainer

from app.events.outbox_publisher import _build_message, publish_batch
from app.models.outbox import OutboxEvent
from app.repositories.outbox_repository import OutboxRepository


@pytest.fixture(scope="session")
def kafka_container():
    """Ephemeral Kafka on a random free host port — same rationale as
    postgres_container/minio_container in this directory's own conftest.py:
    isolated, nothing persists after the test session."""
    with KafkaContainer() as container:
        yield container


@pytest_asyncio.fixture
async def kafka_producer(kafka_container: KafkaContainer) -> AsyncGenerator[AIOKafkaProducer, None]:
    producer = AIOKafkaProducer(bootstrap_servers=kafka_container.get_bootstrap_server())
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


async def _insert_outbox_row(db: AsyncSession, *, event_type: str = "file.uploaded") -> OutboxEvent:
    document_id = uuid.uuid4()
    payload = {
        "document_id": str(document_id),
        "conversation_id": None,
        "owner_id": str(uuid.uuid4()),
        "filename": "physics.pdf",
        "content_type": "application/pdf",
        "storage": {
            "provider": "minio",
            "bucket": "graphgpt-files",
            "key": f"owner/{document_id}/v1/original.pdf",
            "etag": "d41d8cd98f00b204e9800998ecf8427e",
        },
        "timestamp": "2026-09-06T15:00:00+00:00",
    }
    row = await OutboxRepository.insert(db, aggregate_id=document_id, event_type=event_type, payload=payload)
    await db.commit()
    return row


async def _consume_matching(
    kafka_container: KafkaContainer, topic: str, expected_event_id: str, *, count: int = 1, timeout_ms: int = 20_000
) -> list[dict]:
    """Reads from the beginning of `topic` (a fresh, unique consumer group
    per call, so `auto_offset_reset='earliest'` always works regardless of
    when the consumer starts relative to the producer) and returns the first
    `count` messages whose event_id matches `expected_event_id`.

    The topic itself is shared across every test in this module (one
    session-scoped Kafka container, docs/LLD.md's topic name == event_type
    with no per-test namespacing), so a naive "read the first message" would
    pick up leftovers from an earlier test — filtering by event_id is what
    makes each test's assertions robust to that shared state.
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_container.get_bootstrap_server(),
        group_id=f"test-{uuid.uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    matched: list[dict] = []
    try:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout_ms / 1000
        while len(matched) < count and loop.time() < deadline:
            remaining_ms = max(int((deadline - loop.time()) * 1000), 1)
            result = await consumer.getmany(timeout_ms=remaining_ms, max_records=50)
            for records in result.values():
                for record in records:
                    message = json.loads(record.value)
                    if message.get("event_id") == expected_event_id:
                        matched.append(message)
        if len(matched) < count:
            raise AssertionError(
                f"Only observed {len(matched)}/{count} message(s) for event_id={expected_event_id} "
                f"on topic '{topic}' within {timeout_ms}ms"
            )
        return matched
    finally:
        await consumer.stop()


async def test_worker_publishes_and_marks_processed(
    db_session: AsyncSession, kafka_container: KafkaContainer, kafka_producer: AIOKafkaProducer
) -> None:
    row = await _insert_outbox_row(db_session)

    published_count = await publish_batch(db_session, kafka_producer)
    assert published_count == 1

    refreshed = (await db_session.execute(select(OutboxEvent).where(OutboxEvent.id == row.id))).scalar_one()
    assert refreshed.processed is True

    (message,) = await _consume_matching(kafka_container, "file.uploaded", str(row.id))
    assert message["event_type"] == "file.uploaded"
    assert message["version"] == 1
    assert message["document_id"] == row.payload["document_id"]


async def test_crash_window_between_ack_and_commit_causes_republish(
    migrated_database_url: str, kafka_container: KafkaContainer, kafka_producer: AIOKafkaProducer
) -> None:
    """docs/LLD.md §3.7: if the worker crashes after Kafka ACKs a message but
    before the DB commit that marks it processed, the row is republished on
    the next poll. This is documented, expected at-least-once behavior, not
    a bug — simulated here by rolling back the session that ran the first
    publish_batch instead of letting it commit, then running a second,
    independent publish_batch and asserting the message appears twice."""
    engine = create_async_engine(migrated_database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as insert_session:
        row = await _insert_outbox_row(insert_session)

    # First "attempt": the producer send succeeds (real Kafka ACK), but the
    # session is rolled back instead of committed — simulating a crash in
    # the gap publish_batch's own commit closes in the non-crashing case.
    async with session_factory() as crashing_session:
        rows = await OutboxRepository.fetch_unprocessed_for_update(crashing_session, limit=100)
        assert len(rows) == 1
        await kafka_producer.send_and_wait("file.uploaded", _build_message(rows[0]))
        await OutboxRepository.mark_processed(crashing_session, rows[0])
        await crashing_session.rollback()  # simulated crash — processed=TRUE never commits

    # The row is still processed=FALSE from the DB's perspective, so a
    # second, independent publish_batch republishes it for real.
    async with session_factory() as retry_session:
        published_count = await publish_batch(retry_session, kafka_producer)
    assert published_count == 1

    async with session_factory() as verify_session:
        refreshed = (await verify_session.execute(select(OutboxEvent).where(OutboxEvent.id == row.id))).scalar_one()
    assert refreshed.processed is True

    await engine.dispose()

    # Both the simulated-crash send and the real retry landed on the topic —
    # proof the event was delivered at least once, twice in this case.
    first, second = await _consume_matching(kafka_container, "file.uploaded", str(row.id), count=2)
    assert first["event_id"] == second["event_id"] == str(row.id)


async def test_concurrent_workers_never_double_publish_the_same_row(
    migrated_database_url: str, kafka_producer: AIOKafkaProducer
) -> None:
    """docs/LLD.md §4.7: `FOR UPDATE SKIP LOCKED` proof — two worker
    instances racing the same backlog must publish each row exactly once
    between them, never both publishing the same row."""
    engine = create_async_engine(migrated_database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as insert_session:
        for _ in range(10):
            await _insert_outbox_row(insert_session)

    async with session_factory() as session_a, session_factory() as session_b:
        published_a, published_b = await asyncio.gather(
            publish_batch(session_a, kafka_producer, limit=100),
            publish_batch(session_b, kafka_producer, limit=100),
        )

    assert published_a + published_b == 10  # every row published exactly once in total

    await engine.dispose()
