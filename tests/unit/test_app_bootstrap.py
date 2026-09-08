"""Phase 0 smoke test: the app module imports cleanly and exposes /health/live.

Later phases add real unit tests (test_key_builder.py, test_object_verifier.py,
test_lifecycle_service.py, test_permission_service.py, per docs/IMPLEMENTATION_PLAN.md
Phase 2/4) — this file only proves the bootstrap has no import-time errors.

`bootstrap_buckets` is mocked out here: since Phase 2 wired it into `lifespan()`,
this test would otherwise require a live MinIO to boot the app at all, which
would make it an integration test in disguise. Real bootstrap_buckets
correctness (including idempotency) is covered by
tests/integration/test_storage.py::test_bucket_bootstrap_is_idempotent.

Phase 5 wired a Kafka producer + the outbox_worker background task into the
same `lifespan()`, so `start_producer`/`stop_producer`/`run_outbox_worker`
are mocked out here for the identical reason — this is a bootstrap smoke
test, not the place to require a live Kafka broker. Real outbox-publishing
correctness is covered by tests/integration/test_outbox_publishing.py
against a real, ephemeral Kafka (testcontainers).
"""

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.main import app


def test_health_live_returns_200() -> None:
    with (
        patch("app.lifespan.bootstrap_buckets"),
        patch("app.lifespan.start_producer", AsyncMock(return_value=AsyncMock())),
        patch("app.lifespan.stop_producer", AsyncMock()),
        patch("app.lifespan.run_outbox_worker", AsyncMock()),
    ):
        with TestClient(app) as client:
            response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive"}
