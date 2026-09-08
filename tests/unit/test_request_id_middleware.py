from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.main import app


def test_generates_a_request_id_when_none_supplied() -> None:
    with (
        patch("app.lifespan.bootstrap_buckets"),
        patch("app.lifespan.start_producer", AsyncMock(return_value=AsyncMock())),
        patch("app.lifespan.stop_producer", AsyncMock()),
        patch("app.lifespan.run_outbox_worker", AsyncMock()),
        patch("app.lifespan.run_processing_status_consumer", AsyncMock()),
        patch("app.lifespan.run_reconciliation_worker", AsyncMock()),
    ):
        with TestClient(app) as client:
            response = client.get("/health/live")

    assert response.status_code == 200
    assert "X-Request-ID" in response.headers
    assert response.headers["X-Request-ID"]  # non-empty


def test_echoes_back_a_client_supplied_request_id() -> None:
    with (
        patch("app.lifespan.bootstrap_buckets"),
        patch("app.lifespan.start_producer", AsyncMock(return_value=AsyncMock())),
        patch("app.lifespan.stop_producer", AsyncMock()),
        patch("app.lifespan.run_outbox_worker", AsyncMock()),
        patch("app.lifespan.run_processing_status_consumer", AsyncMock()),
        patch("app.lifespan.run_reconciliation_worker", AsyncMock()),
    ):
        with TestClient(app) as client:
            response = client.get("/health/live", headers={"X-Request-ID": "caller-supplied-id-123"})

    assert response.headers["X-Request-ID"] == "caller-supplied-id-123"
