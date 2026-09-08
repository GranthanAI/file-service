"""Phase 7 exit-criteria coverage for POST /files/{document_id}/retry
(docs/LLD.md §4.4/§5.7): a FAILED file moves to PROCESSING and republishes
file.processing_retry; a non-FAILED file gets 409; permission enforcement
matches the EDITOR-level rule documented in retry_service.py."""

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import FileStatus, PermissionLevel
from app.models.outbox import OutboxEvent
from app.repositories.file_permission_repository import FilePermissionRepository
from app.repositories.file_repository import FileRepository
from app.utils.id_generator import new_uuid7


def _auth_headers_for(user_id: UUID) -> dict[str, str]:
    token = jwt.encode(
        {"sub": str(user_id), "exp": datetime.now(timezone.utc) + timedelta(minutes=15)},
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    return {"Authorization": f"Bearer {token}"}


async def _insert_file(db_session: AsyncSession, owner_id: UUID, *, status: FileStatus, retry_count: int = 0) -> UUID:
    document_id = new_uuid7()
    file_obj = await FileRepository.insert(
        db_session,
        document_id=document_id,
        conversation_id=None,
        owner_id=owner_id,
        filename="physics.pdf",
        content_type="application/pdf",
        size_bytes=1024,
        storage_bucket="graphgpt-files",
        storage_key=f"{owner_id}/{document_id}/v1/original.pdf",
    )
    await FileRepository.update_status(db_session, file_obj, FileStatus.UPLOADED, etag="fake-etag")
    await FileRepository.update_status(db_session, file_obj, FileStatus.PROCESSING)
    if status == FileStatus.FAILED:
        await FileRepository.update_status(db_session, file_obj, FileStatus.FAILED, last_error_code="PARSE_ERROR")
    elif status == FileStatus.READY:
        await FileRepository.update_status(db_session, file_obj, FileStatus.READY)
    for _ in range(retry_count):
        await FileRepository.increment_retry_count(db_session, file_obj)
    await db_session.commit()
    return document_id


async def test_owner_can_retry_failed_file(app_client, db_session, current_user_id) -> None:
    document_id = await _insert_file(db_session, current_user_id, status=FileStatus.FAILED)
    auth_headers = _auth_headers_for(current_user_id)

    response = app_client.post(f"/files/{document_id}/retry", headers=auth_headers)

    assert response.status_code == 202, response.text
    body = response.json()
    assert body["status"] == FileStatus.PROCESSING.value
    assert body["retry_count"] == 1

    events = (
        (await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id == document_id)))
        .scalars()
        .all()
    )
    retry_events = [e for e in events if e.event_type == "file.processing_retry"]
    assert len(retry_events) == 1
    assert retry_events[0].payload["retry_count"] == 1


async def test_retry_on_non_failed_file_returns_409(app_client, db_session, current_user_id) -> None:
    document_id = await _insert_file(db_session, current_user_id, status=FileStatus.READY)
    auth_headers = _auth_headers_for(current_user_id)

    response = app_client.post(f"/files/{document_id}/retry", headers=auth_headers)

    assert response.status_code == 409
    assert response.json()["error_code"] == "RetryNotAllowedError"


async def test_stranger_gets_403(app_client, db_session, current_user_id) -> None:
    document_id = await _insert_file(db_session, current_user_id, status=FileStatus.FAILED)
    stranger_headers = _auth_headers_for(uuid4())

    response = app_client.post(f"/files/{document_id}/retry", headers=stranger_headers)

    assert response.status_code == 403


async def test_editor_grant_allows_retry(app_client, db_session, current_user_id) -> None:
    owner_id = current_user_id
    document_id = await _insert_file(db_session, owner_id, status=FileStatus.FAILED)

    editor_id = uuid4()
    await FilePermissionRepository.upsert(
        db_session, document_id=document_id, principal_id=editor_id, access_level=PermissionLevel.EDITOR, granted_by=owner_id
    )
    await db_session.commit()

    response = app_client.post(f"/files/{document_id}/retry", headers=_auth_headers_for(editor_id))
    assert response.status_code == 202, response.text


async def test_viewer_grant_is_not_enough(app_client, db_session, current_user_id) -> None:
    owner_id = current_user_id
    document_id = await _insert_file(db_session, owner_id, status=FileStatus.FAILED)

    viewer_id = uuid4()
    await FilePermissionRepository.upsert(
        db_session, document_id=document_id, principal_id=viewer_id, access_level=PermissionLevel.VIEWER, granted_by=owner_id
    )
    await db_session.commit()

    response = app_client.post(f"/files/{document_id}/retry", headers=_auth_headers_for(viewer_id))
    assert response.status_code == 403
