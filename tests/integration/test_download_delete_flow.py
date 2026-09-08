"""Phase 8 exit-criteria test (docs/IMPLEMENTATION_PLAN.md): the full
lifecycle `upload -> download (VIEWER can, stranger 403) -> delete ->
subsequent GET returns 404`, against a real (ephemeral) Postgres + MinIO,
over the real HTTP API — matching test_upload_flow.py's own AV-disabled dev
flow so the file reaches UPLOADED without depending on Phase 6's quarantine
consumer.
"""

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import httpx
import jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import FileStatus, PermissionLevel
from app.models.outbox import OutboxEvent
from app.repositories.file_repository import FileRepository

CONTENT = b"%PDF-1.4 fake pdf content for the phase 8 download/delete flow test"


def _auth_headers_for(user_id: UUID) -> dict[str, str]:
    token = jwt.encode(
        {"sub": str(user_id), "exp": datetime.now(timezone.utc) + timedelta(minutes=15)},
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    return {"Authorization": f"Bearer {token}"}


def _upload_and_complete(app_client, auth_headers) -> UUID:
    initiate_resp = app_client.post(
        "/files/upload/initiate",
        json={"filename": "physics.pdf", "content_type": "application/pdf", "size_bytes": len(CONTENT)},
        headers=auth_headers,
    )
    assert initiate_resp.status_code == 201, initiate_resp.text
    initiated = initiate_resp.json()
    document_id = UUID(initiated["document_id"])

    files = {"file": ("original.pdf", CONTENT)}
    upload_resp = httpx.post(initiated["upload_url"], data=initiated["form_data"], files=files)
    assert upload_resp.status_code in (200, 204), upload_resp.text

    complete_resp = app_client.post(f"/files/{document_id}/complete", headers=auth_headers)
    assert complete_resp.status_code == 200, complete_resp.text

    return document_id


async def test_full_lifecycle_upload_download_delete_then_404(
    app_client, db_session: AsyncSession, current_user_id
) -> None:
    owner_id = current_user_id
    owner_headers = _auth_headers_for(owner_id)
    document_id = _upload_and_complete(app_client, owner_headers)

    # Owner can download.
    owner_download = app_client.get(f"/files/{document_id}/download", headers=owner_headers)
    assert owner_download.status_code == 200, owner_download.text
    body = owner_download.json()
    assert body["download_url"]
    assert body["expires_in"] > 0

    # A stranger cannot.
    stranger_headers = _auth_headers_for(uuid4())
    stranger_download = app_client.get(f"/files/{document_id}/download", headers=stranger_headers)
    assert stranger_download.status_code == 403

    # A VIEWER grant makes the stranger a legitimate viewer.
    viewer_id = uuid4()
    grant_resp = app_client.put(
        f"/files/{document_id}/permissions",
        json={"principal_id": str(viewer_id), "access_level": PermissionLevel.VIEWER.value},
        headers=owner_headers,
    )
    assert grant_resp.status_code == 200, grant_resp.text
    viewer_download = app_client.get(f"/files/{document_id}/download", headers=_auth_headers_for(viewer_id))
    assert viewer_download.status_code == 200, viewer_download.text

    # The downloaded content matches what was uploaded, via the presigned URL.
    get_resp = httpx.get(viewer_download.json()["download_url"])
    assert get_resp.status_code == 200
    assert get_resp.content == CONTENT

    # docs/LLD.md §4.2: DELETING is only reachable from READY/FAILED —
    # app_client's fixture mocks out run_processing_status_consumer (Phase 7),
    # so nothing would otherwise drive this file past UPLOADED. Simulate the
    # rest of the real pipeline directly against the DB, exactly like a real
    # deployment's processing_status_consumer would (UPLOADED -> PROCESSING
    # -> READY), so the delete step below exercises a realistic precondition.
    file_obj = await FileRepository.get(db_session, document_id)
    await FileRepository.update_status(db_session, file_obj, FileStatus.PROCESSING)
    await FileRepository.update_status(db_session, file_obj, FileStatus.READY)
    await db_session.commit()

    # A viewer cannot delete (owner-only per docs/LLD.md §5.8).
    viewer_delete = app_client.delete(f"/files/{document_id}", headers=_auth_headers_for(viewer_id))
    assert viewer_delete.status_code == 403

    # Owner deletes.
    delete_resp = app_client.delete(f"/files/{document_id}", headers=owner_headers)
    assert delete_resp.status_code == 202, delete_resp.text
    assert delete_resp.json()["status"] == "DELETED"

    # Subsequent GET is 404 — soft-deleted files are treated as not found
    # (docs/LLD.md §5.3, matching permission_service.assert_access).
    get_after_delete = app_client.get(f"/files/{document_id}", headers=owner_headers)
    assert get_after_delete.status_code == 404

    events = (
        (await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id == document_id)))
        .scalars()
        .all()
    )
    assert any(e.event_type == "file.deleted" for e in events)
