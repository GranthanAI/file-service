from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import jwt
import pytest
import pytest_asyncio
from aiokafka import AIOKafkaProducer
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.minio import MinioContainer
from testcontainers.community.postgres import PostgresContainer

from app.core.config import settings


def _project_root() -> str:
    import os

    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def postgres_container():
    """Spins up a throwaway, ephemeral Postgres container on a random free
    host port (avoids any conflict with a host-side Postgres on 5432 — see
    README.md's troubleshooting note) — testcontainers manages its full
    lifecycle, nothing persists after the test session."""
    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def database_url(postgres_container: PostgresContainer) -> str:
    sync_url = postgres_container.get_connection_url()  # postgresql+psycopg2://...
    return sync_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://")


@pytest.fixture(scope="session")
def migrated_database_url(database_url: str) -> str:
    """Runs the real Alembic migration chain (app/db/migrations) against the
    ephemeral container, so these tests exercise the same schema production
    uses — not a hand-rolled `create_all()`."""
    alembic_cfg = Config(f"{_project_root()}/alembic.ini")
    alembic_cfg.set_main_option("script_location", f"{_project_root()}/app/db/migrations")
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_cfg, "head")
    return database_url


@pytest_asyncio.fixture
async def db_session(migrated_database_url: str) -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(migrated_database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
        await session.rollback()
    await engine.dispose()


@pytest.fixture(scope="session")
def minio_container():
    """Ephemeral MinIO on a random free host port — same rationale as
    postgres_container: isolated, no conflict with the dedicated `file-minio`
    dev container, nothing persists after the test session.

    MINIO_KMS_SECRET_KEY enables the built-in single-key KMS so
    bucket_bootstrap's set_bucket_encryption (SSE-S3) succeeds — modern
    MinIO rejects SSE-S3 with "KMS is not configured" otherwise. Same
    dev-only key used in the root docker-compose.yml's file-minio service,
    not a production secret.
    """
    # Pinned to the MinIO release the shared stack runs (root docker-compose.yml
    # file-minio). minio/minio is no longer published on Docker Hub, so relying on
    # testcontainers' default tag breaks as soon as that image is not cached locally.
    container = MinioContainer(image="minio/minio:RELEASE.2023-03-20T20-16-18Z").with_env(
        "MINIO_KMS_SECRET_KEY", "test-key:SEg942nDjeIcAe34dNb+Dl/Y8QOOeUrpdgKRxTAtEDQ="
    )
    with container as started:
        yield started


@pytest.fixture
def minio_client(minio_container: MinioContainer) -> Minio:
    return minio_container.get_client()


@pytest.fixture
def minio_test_bucket(minio_client: Minio) -> str:
    bucket = "test-bucket"
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
    return bucket


@pytest.fixture(scope="session")
def kafka_container():
    """Ephemeral Kafka on a random free host port — same rationale as
    postgres_container/minio_container above: isolated, nothing persists
    after the test session. Shared (session-scoped) across every test file
    that needs a real Kafka broker (test_outbox_publishing.py,
    test_processing_status_flow.py) so its startup cost is paid once."""
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


@pytest.fixture
def current_user_id() -> UUID:
    return uuid4()


@pytest.fixture
def auth_headers(current_user_id: UUID) -> dict[str, str]:
    """A real JWT signed with the same JWT_SECRET_KEY/JWT_ALGORITHM the app
    verifies with (docs/LLD.md's auth pattern, mirroring auth-service's own
    HS256 implementation) — exercises the real get_current_user_id
    verification path rather than mocking it out."""
    token = jwt.encode(
        {"sub": str(current_user_id), "exp": datetime.now(timezone.utc) + timedelta(minutes=15)},
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def app_client(migrated_database_url: str, minio_client: Minio):
    """A TestClient wired to the ephemeral Postgres + MinIO from the fixtures
    above, via FastAPI dependency_overrides — NOT the app's real
    settings-configured DB/MinIO. bootstrap_buckets is mocked (it's already
    covered by tests/integration/test_storage.py::test_bucket_bootstrap_is_idempotent)
    since it isn't reachable via dependency_overrides (lifespan calls it
    directly, not through Depends()) — the two buckets it would have created
    are created directly on the test MinIO instead.

    start_producer/stop_producer/run_outbox_worker are mocked out the same
    way: these HTTP-flow tests only assert on `outbox_events` DB rows (see
    e.g. test_upload_flow.py), never on what actually reaches Kafka, so
    booting a real producer/worker here would just be an unused dependency
    on a live broker. Real outbox-publishing behavior is covered by
    tests/integration/test_outbox_publishing.py against a real, ephemeral
    Kafka (testcontainers). run_processing_status_consumer (Phase 7) and
    run_reconciliation_worker (Phase 9) are mocked for the same reason —
    neither is gated behind AV_SCAN_ENABLED and both would otherwise need a
    live broker for every HTTP-flow test in this fixture, not just
    quarantine-specific ones.
    """
    from app.api.deps import get_db
    from app.main import app
    from app.storage.minio_client import get_minio_client

    for bucket in (settings.BUCKET_FILES, settings.BUCKET_QUARANTINE):
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)

    engine = create_async_engine(migrated_database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async def _override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_minio_client] = lambda: minio_client

    with (
        patch("app.lifespan.bootstrap_buckets"),
        patch("app.lifespan.start_producer", AsyncMock(return_value=AsyncMock())),
        patch("app.lifespan.stop_producer", AsyncMock()),
        patch("app.lifespan.run_outbox_worker", AsyncMock()),
        patch("app.lifespan.run_processing_status_consumer", AsyncMock()),
        patch("app.lifespan.run_reconciliation_worker", AsyncMock()),
    ):
        with TestClient(app) as client:
            yield client

    app.dependency_overrides.clear()
