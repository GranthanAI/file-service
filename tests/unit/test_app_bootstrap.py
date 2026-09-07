"""Phase 0 smoke test: the app module imports cleanly and exposes /health/live.

Later phases add real unit tests (test_key_builder.py, test_object_verifier.py,
test_lifecycle_service.py, test_permission_service.py, per docs/IMPLEMENTATION_PLAN.md
Phase 2/4) — this file only proves the bootstrap has no import-time errors.

`bootstrap_buckets` is mocked out here: since Phase 2 wired it into `lifespan()`,
this test would otherwise require a live MinIO to boot the app at all, which
would make it an integration test in disguise. Real bootstrap_buckets
correctness (including idempotency) is covered by
tests/integration/test_storage.py::test_bucket_bootstrap_is_idempotent.
"""

from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


def test_health_live_returns_200() -> None:
    with patch("app.lifespan.bootstrap_buckets"):
        with TestClient(app) as client:
            response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive"}
