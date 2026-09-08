"""Phase 4 exit-criteria tests (docs/IMPLEMENTATION_PLAN.md): the permission
matrix (owner / VIEWER grant / stranger) and list-pagination round trip
through the real HTTP API, against a real (ephemeral) Postgres + MinIO."""

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import jwt

from app.core.config import settings
from app.core.enums import FileStatus
from app.repositories.file_repository import FileRepository
from app.utils.id_generator import new_uuid7


def _auth_headers_for(user_id: UUID) -> dict[str, str]:
    token = jwt.encode(
        {"sub": str(user_id), "exp": datetime.now(timezone.utc) + timedelta(minutes=15)},
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    return {"Authorization": f"Bearer {token}"}


async def _insert_file(db_session, owner_id: UUID, **overrides) -> UUID:
    document_id = new_uuid7()
    kwargs = dict(
        document_id=document_id,
        conversation_id=uuid4(),
        owner_id=owner_id,
        filename="physics.pdf",
        content_type="application/pdf",
        size_bytes=1024,
        storage_bucket="graphgpt-files",
        storage_key=f"{owner_id}/{document_id}/v1/original.pdf",
    )
    kwargs.update(overrides)
    await FileRepository.insert(db_session, **kwargs)
    await db_session.commit()
    return document_id


class TestPermissionMatrix:
    async def test_owner_can_get_file(self, app_client, db_session, current_user_id) -> None:
        document_id = await _insert_file(db_session, current_user_id)
        auth_headers = _auth_headers_for(current_user_id)

        response = app_client.get(f"/files/{document_id}", headers=auth_headers)

        assert response.status_code == 200, response.text
        assert response.json()["document_id"] == str(document_id)

    async def test_stranger_gets_403_on_get(self, app_client, db_session, current_user_id) -> None:
        owner_id = current_user_id
        document_id = await _insert_file(db_session, owner_id)
        stranger_headers = _auth_headers_for(uuid4())

        response = app_client.get(f"/files/{document_id}", headers=stranger_headers)

        assert response.status_code == 403
        assert response.json()["error_code"] == "PermissionDeniedError"

    async def test_stranger_gets_403_on_status(self, app_client, db_session, current_user_id) -> None:
        document_id = await _insert_file(db_session, current_user_id)
        stranger_headers = _auth_headers_for(uuid4())

        response = app_client.get(f"/files/{document_id}/status", headers=stranger_headers)

        assert response.status_code == 403

    async def test_stranger_gets_403_on_put_permissions(self, app_client, db_session, current_user_id) -> None:
        document_id = await _insert_file(db_session, current_user_id)
        stranger_headers = _auth_headers_for(uuid4())

        response = app_client.put(
            f"/files/{document_id}/permissions",
            json={"principal_id": str(uuid4()), "access_level": "VIEWER"},
            headers=stranger_headers,
        )

        assert response.status_code == 403

    async def test_viewer_grant_allows_get_but_not_put_permissions(
        self, app_client, db_session, current_user_id
    ) -> None:
        owner_id = current_user_id
        owner_headers = _auth_headers_for(owner_id)
        document_id = await _insert_file(db_session, owner_id)

        viewer_id = uuid4()
        grant_resp = app_client.put(
            f"/files/{document_id}/permissions",
            json={"principal_id": str(viewer_id), "access_level": "VIEWER"},
            headers=owner_headers,
        )
        assert grant_resp.status_code == 200, grant_resp.text
        assert grant_resp.json()["access_level"] == "VIEWER"

        viewer_headers = _auth_headers_for(viewer_id)

        get_resp = app_client.get(f"/files/{document_id}", headers=viewer_headers)
        assert get_resp.status_code == 200, get_resp.text

        status_resp = app_client.get(f"/files/{document_id}/status", headers=viewer_headers)
        assert status_resp.status_code == 200, status_resp.text

        put_perm_resp = app_client.put(
            f"/files/{document_id}/permissions",
            json={"principal_id": str(uuid4()), "access_level": "VIEWER"},
            headers=viewer_headers,
        )
        assert put_perm_resp.status_code == 403
        assert put_perm_resp.json()["error_code"] == "PermissionDeniedError"

    async def test_unknown_document_id_returns_404(self, app_client, auth_headers) -> None:
        response = app_client.get(f"/files/{uuid4()}", headers=auth_headers)
        assert response.status_code == 404
        assert response.json()["error_code"] == "FileRecordNotFoundError"

    async def test_soft_deleted_file_returns_404_even_for_owner(self, app_client, db_session, current_user_id) -> None:
        document_id = await _insert_file(db_session, current_user_id)
        file_obj = await FileRepository.get(db_session, document_id)
        await FileRepository.update_status(db_session, file_obj, FileStatus.FAILED)
        await FileRepository.update_status(db_session, file_obj, FileStatus.DELETING)
        await FileRepository.update_status(db_session, file_obj, FileStatus.DELETED)
        await db_session.commit()

        response = app_client.get(f"/files/{document_id}", headers=_auth_headers_for(current_user_id))
        assert response.status_code == 404


class TestListPagination:
    async def test_25_files_paginate_in_3_pages_with_no_duplicates_or_gaps(
        self, app_client, db_session, current_user_id
    ) -> None:
        owner_id = current_user_id
        auth_headers = _auth_headers_for(owner_id)
        expected_ids = set()
        for _ in range(25):
            document_id = await _insert_file(db_session, owner_id)
            expected_ids.add(str(document_id))

        seen_ids: list[str] = []
        cursor = None
        pages = 0
        while True:
            params = {"limit": 10}
            if cursor is not None:
                params["cursor"] = cursor
            response = app_client.get("/files", params=params, headers=auth_headers)
            assert response.status_code == 200, response.text
            body = response.json()
            pages += 1
            seen_ids.extend(item["document_id"] for item in body["items"])
            cursor = body["next_cursor"]
            if cursor is None:
                break
            assert pages <= 10  # safety valve against an infinite loop on a bug

        assert pages == 3
        assert len(seen_ids) == len(expected_ids) == 25  # no duplicates, no gaps
        assert set(seen_ids) == expected_ids
