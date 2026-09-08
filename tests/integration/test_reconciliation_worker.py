"""Phase 9 exit-criteria tests (docs/IMPLEMENTATION_PLAN.md): one
reconciliation sweep expires stale upload_jobs and fails abandoned
UPLOADING files, against a real (ephemeral) Postgres.
"""

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import FileStatus, UploadJobStatus
from app.repositories.file_repository import FileRepository
from app.repositories.upload_job_repository import UploadJobRepository
from app.utils.id_generator import new_uuid7
from app.workers.reconciliation_worker import run_sweep


async def _insert_uploading_file(db: AsyncSession, *, created_at: datetime | None = None):
    document_id = new_uuid7()
    owner_id = uuid.uuid4()
    file_obj = await FileRepository.insert(
        db,
        document_id=document_id,
        conversation_id=None,
        owner_id=owner_id,
        filename="physics.pdf",
        content_type="application/pdf",
        size_bytes=1024,
        storage_bucket="graphgpt-files",
        storage_key=f"{owner_id}/{document_id}/v1/original.pdf",
    )
    if created_at is not None:
        file_obj.created_at = created_at
        db.add(file_obj)
        await db.flush()
    return file_obj


async def test_expired_upload_job_flips_to_expired(db_session: AsyncSession) -> None:
    file_obj = await _insert_uploading_file(db_session)
    job = await UploadJobRepository.insert(
        db_session,
        document_id=file_obj.document_id,
        presign_method="PUT",
        expected_size=1024,
        expected_content_type="application/pdf",
        expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )
    await db_session.commit()

    summary = await run_sweep(db_session)

    assert summary.expired_upload_jobs >= 1

    await db_session.refresh(job)
    assert job.status == UploadJobStatus.EXPIRED.value


async def test_abandoned_uploading_file_flips_to_failed(db_session: AsyncSession) -> None:
    old_created_at = datetime.now(timezone.utc) - timedelta(hours=2)
    file_obj = await _insert_uploading_file(db_session, created_at=old_created_at)
    await db_session.commit()

    summary = await run_sweep(db_session)

    assert summary.abandoned_files_failed >= 1

    refreshed = await FileRepository.get(db_session, file_obj.document_id)
    assert refreshed.status == FileStatus.FAILED
    assert refreshed.last_error_code == "ABANDONED_UPLOAD"


async def test_recent_uploading_file_is_left_alone(db_session: AsyncSession) -> None:
    """docs/LLD.md §4.6 step 2 — only files older than the 1h timeout are
    touched; a file mid-upload seconds ago must not be prematurely failed."""
    file_obj = await _insert_uploading_file(db_session)
    await db_session.commit()

    await run_sweep(db_session)

    refreshed = await FileRepository.get(db_session, file_obj.document_id)
    assert refreshed.status == FileStatus.UPLOADING


async def test_uploading_file_with_a_pending_job_is_left_alone(db_session: AsyncSession) -> None:
    """A file that's UPLOADING with an active (PENDING) upload_jobs row —
    even if old — is a legitimate in-flight upload, not abandoned."""
    old_created_at = datetime.now(timezone.utc) - timedelta(hours=2)
    file_obj = await _insert_uploading_file(db_session, created_at=old_created_at)
    await UploadJobRepository.insert(
        db_session,
        document_id=file_obj.document_id,
        presign_method="PUT",
        expected_size=1024,
        expected_content_type="application/pdf",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=15),
    )
    await db_session.commit()

    await run_sweep(db_session)

    refreshed = await FileRepository.get(db_session, file_obj.document_id)
    assert refreshed.status == FileStatus.UPLOADING


def test_reconciliation_worker_never_calls_list_objects() -> None:
    """docs/LLD.md §4.6's explicit out-of-scope note, enforced as a static
    check: no full-bucket LIST anywhere in this module's actual code (the
    module's own docstring names the forbidden call to explain why it's
    forbidden, so the docstring itself is excluded from the scan)."""
    import ast
    import inspect

    from app.workers import reconciliation_worker

    tree = ast.parse(inspect.getsource(reconciliation_worker))
    tree.body = tree.body[1:]  # drop the module docstring (an ast.Expr node)
    code_without_docstring = ast.unparse(tree)

    assert "list_objects" not in code_without_docstring
    assert "ListObjectsV2" not in code_without_docstring
