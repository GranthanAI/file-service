"""Phase 3 exit-criteria tests (docs/IMPLEMENTATION_PLAN.md): the full
initiate -> PUT-to-MinIO -> complete round trip through the real HTTP API,
against a real (ephemeral) Postgres + MinIO, with AV_SCAN_ENABLED=false
(the .env default for this test environment)."""

from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import FileStatus
from app.models.file import File
from app.models.file_version import FileVersion
from app.models.outbox import OutboxEvent

CONTENT = b"%PDF-1.4 fake pdf content for the phase 3 upload flow test"


def _initiate(app_client, auth_headers, *, filename: str = "physics.pdf", size_bytes: int | None = None) -> dict:
    response = app_client.post(
        "/files/upload/initiate",
        json={
            "filename": filename,
            "content_type": "application/pdf",
            "size_bytes": size_bytes if size_bytes is not None else len(CONTENT),
        },
        headers=auth_headers,
    )
    assert response.status_code == 201, response.text
    return response.json()


def _upload_bytes(initiate_response: dict, content: bytes = CONTENT) -> None:
    files = {"file": ("original.pdf", content)}
    resp = httpx.post(initiate_response["upload_url"], data=initiate_response["form_data"], files=files)
    assert resp.status_code in (200, 204), resp.text


async def test_full_upload_round_trip(app_client, auth_headers, db_session: AsyncSession) -> None:
    initiated = _initiate(app_client, auth_headers)
    document_id = UUID(initiated["document_id"])
    _upload_bytes(initiated)

    complete_resp = app_client.post(f"/files/{document_id}/complete", headers=auth_headers)
    assert complete_resp.status_code == 200, complete_resp.text
    body = complete_resp.json()
    assert body["status"] == FileStatus.UPLOADED.value
    assert body["size_bytes"] == len(CONTENT)
    assert body["etag"]

    file_row = (await db_session.execute(select(File).where(File.document_id == document_id))).scalar_one()
    assert file_row.status == FileStatus.UPLOADED

    versions = (
        (await db_session.execute(select(FileVersion).where(FileVersion.document_id == document_id))).scalars().all()
    )
    assert len(versions) == 1
    assert versions[0].version == 1
    assert versions[0].size_bytes == len(CONTENT)

    events = (
        (
            await db_session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.aggregate_id == document_id, OutboxEvent.event_type == "file.uploaded"
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(events) == 1
    assert events[0].payload["document_id"] == str(document_id)
    assert events[0].processed is False


async def test_complete_is_idempotent(app_client, auth_headers, db_session: AsyncSession) -> None:
    initiated = _initiate(app_client, auth_headers)
    document_id = UUID(initiated["document_id"])
    _upload_bytes(initiated)

    first = app_client.post(f"/files/{document_id}/complete", headers=auth_headers)
    second = app_client.post(f"/files/{document_id}/complete", headers=auth_headers)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()

    versions = (
        (await db_session.execute(select(FileVersion).where(FileVersion.document_id == document_id))).scalars().all()
    )
    assert len(versions) == 1  # not duplicated by the second /complete call

    events = (
        (await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id == document_id))).scalars().all()
    )
    assert len(events) == 1  # not duplicated either


async def test_complete_without_upload_returns_422(app_client, auth_headers) -> None:
    initiated = _initiate(app_client, auth_headers)
    document_id = initiated["document_id"]

    response = app_client.post(f"/files/{document_id}/complete", headers=auth_headers)

    assert response.status_code == 422
    assert response.json()["error_code"] == "ObjectNotFoundError"


async def test_complete_with_wrong_size_returns_422(app_client, auth_headers, db_session: AsyncSession) -> None:
    initiated = _initiate(app_client, auth_headers)
    document_id = UUID(initiated["document_id"])
    # Fewer bytes than declared: still satisfies MinIO's own POST-policy
    # content-length-range ([1, declared_size]), so the upload itself
    # succeeds — it's our own exact-size check in object_verifier.py at
    # /complete that must catch this, not MinIO's policy enforcement
    # (uploading MORE than declared is rejected by MinIO itself before
    # ever reaching file-service, which is a different, storage-layer
    # defense already proven by the EntityTooLarge behavior observed here).
    _upload_bytes(initiated, content=CONTENT[:10])

    response = app_client.post(f"/files/{document_id}/complete", headers=auth_headers)

    assert response.status_code == 422
    assert response.json()["error_code"] == "SizeMismatchError"

    file_row = (await db_session.execute(select(File).where(File.document_id == document_id))).scalar_one()
    assert file_row.status == FileStatus.FAILED
    assert file_row.last_error_code == "SizeMismatchError"


async def test_initiate_rejects_disallowed_content_type(app_client, auth_headers) -> None:
    response = app_client.post(
        "/files/upload/initiate",
        json={"filename": "virus.exe", "content_type": "application/x-msdownload", "size_bytes": 100},
        headers=auth_headers,
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "InvalidContentTypeError"


async def test_initiate_requires_authentication(app_client) -> None:
    response = app_client.post(
        "/files/upload/initiate",
        json={"filename": "a.pdf", "content_type": "application/pdf", "size_bytes": 100},
    )
    assert response.status_code in (401, 403)  # HTTPBearer with no header returns 403 by default


async def test_complete_wrong_owner_returns_403(app_client, auth_headers, current_user_id) -> None:
    import jwt
    from datetime import datetime, timedelta, timezone
    from uuid import uuid4

    from app.core.config import settings

    initiated = _initiate(app_client, auth_headers)
    document_id = initiated["document_id"]
    _upload_bytes(initiated)

    other_user_token = jwt.encode(
        {"sub": str(uuid4()), "exp": datetime.now(timezone.utc) + timedelta(minutes=15)},
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )

    response = app_client.post(
        f"/files/{document_id}/complete", headers={"Authorization": f"Bearer {other_user_token}"}
    )
    assert response.status_code == 403
    assert response.json()["error_code"] == "PermissionDeniedError"
