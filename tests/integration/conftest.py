from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.minio import MinioContainer
from testcontainers.community.postgres import PostgresContainer


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
    container = MinioContainer().with_env(
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
