"""Phase 10 exit-criteria tests (docs/IMPLEMENTATION_PLAN.md): GET /health/ready
returns 503 when Postgres or MinIO is down, and 200 when only Kafka is down
(the readiness fix docs/LLD.md §4.8 documents explicitly) — against the real
HTTP API, real Postgres, real MinIO.

app_client's own fixture clears app.dependency_overrides after every test, so
the extra overrides these tests add to simulate an outage don't need manual
restoring.
"""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession


async def test_ready_returns_200_when_database_and_minio_are_up(app_client) -> None:
    """Also proves the Kafka-down-only case: app_client's fixture never
    starts a real producer/worker/consumer (all mocked — see its own
    docstring), so this genuinely exercises 'Kafka absent, DB+MinIO up'."""
    response = app_client.get("/health/ready")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "ready"
    assert body["database"] is True
    assert body["minio"] is True


async def test_ready_returns_503_when_database_is_down(app_client) -> None:
    from app.api.deps import get_db
    from app.main import app

    async def _broken_get_db() -> AsyncGenerator[AsyncSession, None]:
        session = AsyncMock()
        session.execute.side_effect = ConnectionError("simulated database outage")
        yield session

    app.dependency_overrides[get_db] = _broken_get_db

    response = app_client.get("/health/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["database"] is False


async def test_ready_returns_503_when_minio_is_down(app_client) -> None:
    from app.main import app
    from app.storage.minio_client import get_minio_client

    broken_client = Minio("127.0.0.1:1", access_key="x", secret_key="x", secure=False)
    app.dependency_overrides[get_minio_client] = lambda: broken_client

    response = app_client.get("/health/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["minio"] is False
