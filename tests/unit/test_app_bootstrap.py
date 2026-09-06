"""Phase 0 smoke test: the app module imports cleanly and exposes /health/live.

Later phases add real unit tests (test_key_builder.py, test_object_verifier.py,
test_lifecycle_service.py, test_permission_service.py, per docs/IMPLEMENTATION_PLAN.md
Phase 2/4) — this file only proves the bootstrap has no import-time errors.
"""

from fastapi.testclient import TestClient

from app.main import app


def test_health_live_returns_200() -> None:
    with TestClient(app) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive"}
