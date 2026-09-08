"""Phase 8 exit-criteria coverage for version_service.py (docs/HLD.md §16,
docs/LLD.md §2.3): `upload -> ... -> new version -> list shows both
versions`, against real Postgres + MinIO. version_service has no HTTP route
(see its module docstring for why), so this drives the service functions
directly, the same way test_quarantine_flow.py exercises quarantine_service
directly.
"""

from datetime import datetime, timedelta, timezone
from uuid import UUID

import httpx
import jwt
import pytest
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import FileStatus
from app.core.exceptions import VersioningNotAllowedError
from app.repositories.file_repository import FileRepository
from app.repositories.file_version_repository import FileVersionRepository
from app.services import version_service
from app.utils.id_generator import new_uuid7


def _auth_headers_for(user_id: UUID) -> dict[str, str]:
    token = jwt.encode(
        {"sub": str(user_id), "exp": datetime.now(timezone.utc) + timedelta(minutes=15)},
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    return {"Authorization": f"Bearer {token}"}


V1_CONTENT = b"%PDF-1.4 version one of the test document"
V2_CONTENT = b"%PDF-1.4 version two, a longer revision of the same document"


def _upload_and_complete_v1(app_client, auth_headers) -> UUID:
    initiate_resp = app_client.post(
        "/files/upload/initiate",
        json={"filename": "report.pdf", "content_type": "application/pdf", "size_bytes": len(V1_CONTENT)},
        headers=auth_headers,
    )
    assert initiate_resp.status_code == 201, initiate_resp.text
    initiated = initiate_resp.json()
    document_id = UUID(initiated["document_id"])

    upload_resp = httpx.post(
        initiated["upload_url"], data=initiated["form_data"], files={"file": ("original.pdf", V1_CONTENT)}
    )
    assert upload_resp.status_code in (200, 204), upload_resp.text

    complete_resp = app_client.post(f"/files/{document_id}/complete", headers=auth_headers)
    assert complete_resp.status_code == 200, complete_resp.text

    return document_id


async def test_new_version_completes_and_both_versions_are_listed(
    app_client, db_session: AsyncSession, minio_client: Minio, current_user_id
) -> None:
    owner_id = current_user_id
    owner_headers = _auth_headers_for(owner_id)
    document_id = _upload_and_complete_v1(app_client, owner_headers)

    # Drive the file to READY — app_client's fixture mocks out the real
    # processing_status_consumer (Phase 7), so simulate the rest of the
    # pipeline directly, exactly like test_download_delete_flow.py does.
    file_obj = await FileRepository.get(db_session, document_id)
    await FileRepository.update_status(db_session, file_obj, FileStatus.PROCESSING)
    await FileRepository.update_status(db_session, file_obj, FileStatus.READY)
    await db_session.commit()

    # Initiate + upload + complete v2, entirely at the service layer.
    result = await version_service.initiate_new_version(
        db_session,
        minio_client,
        document_id=document_id,
        requester_id=owner_id,
        filename="report_updated.pdf",
        content_type="application/pdf",
        size_bytes=len(V2_CONTENT),
    )
    await db_session.commit()
    assert result.version == 2

    upload_resp = httpx.post(
        result.upload_url, data=result.form_data, files={"file": ("original.pdf", V2_CONTENT)}
    )
    assert upload_resp.status_code in (200, 204), upload_resp.text

    completed_file = await version_service.complete_new_version(
        db_session, minio_client, document_id=document_id, requester_id=owner_id
    )
    await db_session.commit()

    assert completed_file.current_version == 2
    assert completed_file.status == FileStatus.PROCESSING

    versions = await FileVersionRepository.list_for_document(db_session, document_id)
    assert [v.version for v in versions] == [1, 2]
    assert versions[0].size_bytes == len(V1_CONTENT)
    assert versions[1].size_bytes == len(V2_CONTENT)

    # v1's object is still present in MinIO — a new version is a NEW key,
    # never an in-place overwrite (docs/HLD.md §16).
    stat_v1 = minio_client.stat_object(settings.BUCKET_FILES, versions[0].storage_key)
    assert stat_v1.size == len(V1_CONTENT)
    stat_v2 = minio_client.stat_object(settings.BUCKET_FILES, versions[1].storage_key)
    assert stat_v2.size == len(V2_CONTENT)


async def test_new_version_rejected_when_file_not_ready(
    db_session: AsyncSession, minio_client: Minio, current_user_id
) -> None:
    """docs/HLD.md §16 — a new version can only be started once the previous
    one is fully processed (READY); attempting it on a file still mid-flight
    (UPLOADING here) is rejected, not silently accepted."""
    document_id = new_uuid7()
    owner_id = current_user_id
    await FileRepository.insert(
        db_session,
        document_id=document_id,
        conversation_id=None,
        owner_id=owner_id,
        filename="report.pdf",
        content_type="application/pdf",
        size_bytes=10,
        storage_bucket="graphgpt-files",
        storage_key=f"{owner_id}/{document_id}/v1/original.pdf",
    )
    await db_session.commit()

    with pytest.raises(VersioningNotAllowedError):
        await version_service.initiate_new_version(
            db_session,
            minio_client,
            document_id=document_id,
            requester_id=owner_id,
            filename="report_v2.pdf",
            content_type="application/pdf",
            size_bytes=10,
        )
