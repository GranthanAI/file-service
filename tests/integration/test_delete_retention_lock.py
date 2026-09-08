"""Phase 8 retention-lock exit criterion (docs/IMPLEMENTATION_PLAN.md):
an object-locked file returns 409 on delete and lands on FAILED, not stuck
in DELETING — against a real, ephemeral MinIO with a genuine Object Lock
retention configured on the object (not simulated).

Uses its own dedicated, uniquely-named, lock-enabled bucket rather than the
shared settings.BUCKET_FILES: Object Locking can only be turned on AT bucket
creation time, and settings.BUCKET_FILES is a session-shared bucket other
tests create without locking — so settings.BUCKET_FILES is patched to this
test's own bucket for its duration, matching how test_processing_status_flow.py
patches settings.KAFKA_BROKERS to reach an ephemeral broker.
"""

import io
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from minio import Minio
from minio.commonconfig import COMPLIANCE
from minio.retention import Retention
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import FileStatus
from app.core.exceptions import RetentionLockedError
from app.repositories.file_repository import FileRepository
from app.repositories.file_version_repository import FileVersionRepository
from app.services.delete_service import delete_file
from app.utils.id_generator import new_uuid7

CONTENT = b"%PDF-1.4 a file under legal hold"


async def test_retention_locked_object_returns_409_and_lands_on_failed(
    db_session: AsyncSession, minio_client: Minio
) -> None:
    locked_bucket = f"test-locked-{uuid.uuid4().hex[:8]}"
    minio_client.make_bucket(locked_bucket, object_lock=True)

    document_id = new_uuid7()
    owner_id = uuid.uuid4()
    key = f"{owner_id}/{document_id}/v1/original.pdf"

    minio_client.put_object(locked_bucket, key, data=io.BytesIO(CONTENT), length=len(CONTENT))
    # COMPLIANCE, not GOVERNANCE: governance-mode retention is bypassable by
    # a caller with s3:BypassGovernanceRetention (which the test's admin
    # MinIO credentials effectively have), so it wouldn't actually block
    # plain remove_object here — compliance mode cannot be bypassed by
    # anyone, including root, until the retention period elapses.
    minio_client.set_object_retention(
        locked_bucket, key, config=Retention(COMPLIANCE, datetime.now(timezone.utc) + timedelta(days=1))
    )

    file_obj = await FileRepository.insert(
        db_session,
        document_id=document_id,
        conversation_id=None,
        owner_id=owner_id,
        filename="physics.pdf",
        content_type="application/pdf",
        size_bytes=len(CONTENT),
        storage_bucket=locked_bucket,
        storage_key=key,
    )
    await FileRepository.update_status(db_session, file_obj, FileStatus.UPLOADED, etag="fake-etag")
    await FileRepository.update_status(db_session, file_obj, FileStatus.PROCESSING)
    await FileRepository.update_status(db_session, file_obj, FileStatus.READY)
    await FileVersionRepository.insert(
        db_session, document_id=document_id, version=1, storage_key=key, etag="fake-etag", size_bytes=len(CONTENT)
    )
    await db_session.commit()

    with patch.object(settings, "BUCKET_FILES", locked_bucket):
        with pytest.raises(RetentionLockedError):
            await delete_file(db_session, minio_client, document_id, owner_id)

    refreshed = await FileRepository.get(db_session, document_id)
    assert refreshed.status == FileStatus.FAILED
    assert refreshed.last_error_code == "RETENTION_LOCKED"

    # The object itself is untouched — retention genuinely blocked removal,
    # not just the DB bookkeeping.
    stat = minio_client.stat_object(locked_bucket, key)
    assert stat.size == len(CONTENT)
