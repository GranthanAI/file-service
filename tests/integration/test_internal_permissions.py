"""Batched internal permission check used by retrieval-service: authorized
subset returned, unauthorized/nonexistent/deleted ids silently omitted, and the
endpoint refuses callers without the service credential."""

from uuid import uuid4

from app.core.config import settings
from app.core.enums import FileStatus, PermissionLevel
from app.repositories.file_permission_repository import FilePermissionRepository
from app.repositories.file_repository import FileRepository

from .test_files_api import _auth_headers_for, _insert_file

URL = "/internal/files/permissions/check"


def _service_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.internal_service_tokens[0]}"}


class TestPermissionCheck:
    async def test_returns_only_authorized_subset(self, app_client, db_session, current_user_id, monkeypatch) -> None:
        monkeypatch.setattr(settings, "INTERNAL_SERVICE_TOKENS", "test-internal-token")
        other = uuid4()
        owned = await _insert_file(db_session, current_user_id)
        granted = await _insert_file(db_session, other)
        await FilePermissionRepository.upsert(
            db_session,
            document_id=granted,
            principal_id=current_user_id,
            access_level=PermissionLevel.VIEWER,
            granted_by=other,
        )
        not_theirs = await _insert_file(db_session, other)
        deleted = await _insert_file(db_session, current_user_id)
        file = await FileRepository.get(db_session, deleted)
        file.status = FileStatus.DELETED  # bypass the state machine: only the filter is under test
        await db_session.commit()

        response = app_client.post(
            URL,
            json={"user_id": str(current_user_id), "document_ids": [str(d) for d in (owned, granted, not_theirs, deleted, uuid4())]},
            headers=_service_headers(),
        )

        assert response.status_code == 200, response.text
        assert set(response.json()["authorized_document_ids"]) == {str(owned), str(granted)}

    async def test_documents_being_deleted_stop_being_retrievable_immediately(
        self, app_client, db_session, current_user_id, monkeypatch
    ) -> None:
        monkeypatch.setattr(settings, "INTERNAL_SERVICE_TOKENS", "test-internal-token")
        deleting = await _insert_file(db_session, current_user_id)
        keep = await _insert_file(db_session, current_user_id)
        file = await FileRepository.get(db_session, deleting)
        file.status = FileStatus.DELETING  # async cleanup still pending
        await db_session.commit()

        response = app_client.post(
            URL,
            json={"user_id": str(current_user_id), "document_ids": [str(deleting), str(keep)]},
            headers=_service_headers(),
        )

        assert response.json()["authorized_document_ids"] == [str(keep)]

    async def test_unknown_user_gets_nothing_even_for_existing_documents(
        self, app_client, db_session, current_user_id, monkeypatch
    ) -> None:
        monkeypatch.setattr(settings, "INTERNAL_SERVICE_TOKENS", "test-internal-token")
        document = await _insert_file(db_session, current_user_id)

        response = app_client.post(
            URL,
            json={"user_id": str(uuid4()), "document_ids": [str(document)]},
            headers=_service_headers(),
        )

        assert response.status_code == 200
        assert response.json()["authorized_document_ids"] == []

    async def test_missing_or_wrong_credential_is_rejected(self, app_client, current_user_id, monkeypatch) -> None:
        monkeypatch.setattr(settings, "INTERNAL_SERVICE_TOKENS", "test-internal-token")
        body = {"user_id": str(current_user_id), "document_ids": []}

        assert app_client.post(URL, json=body).status_code == 401
        assert app_client.post(URL, json=body, headers={"Authorization": "Bearer wrong"}).status_code == 401
        # A valid user JWT is not a service credential.
        assert app_client.post(URL, json=body, headers=_auth_headers_for(current_user_id)).status_code == 401

    async def test_unconfigured_tokens_fail_closed(self, app_client, current_user_id, monkeypatch) -> None:
        monkeypatch.setattr(settings, "INTERNAL_SERVICE_TOKENS", "")
        response = app_client.post(
            URL, json={"user_id": str(current_user_id), "document_ids": []}, headers={"Authorization": "Bearer "}
        )
        assert response.status_code == 401
