from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from minio import Minio
from minio.error import S3Error
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import FileStatus, PermissionLevel
from app.core.exceptions import RetentionLockedError
from app.models.file import File
from app.repositories.file_repository import FileRepository
from app.repositories.file_version_repository import FileVersionRepository
from app.repositories.outbox_repository import OutboxRepository
from app.services import permission_service


def _build_file_deleted_payload(file: File) -> dict[str, Any]:
    """docs/LLD.md §6.5/§15 — no explicit wire shape is documented beyond
    "Insert outbox_events row: file.deleted"; this is the minimal shape
    downstream needs to identify which document to stop indexing."""
    return {
        "document_id": str(file.document_id),
        "owner_id": str(file.owner_id),
        "content_version": file.current_version,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def delete_file(db: AsyncSession, minio_client: Minio, document_id: UUID, requester_id: UUID) -> File:
    """docs/LLD.md §6.5/§15 — requires OWNER (§5.8). Removes every version's
    object from MinIO — `file_versions` (docs/LLD.md §2.3) is the append-only
    source of truth for which keys exist, one object per completed version —
    then marks the row DELETED.

    A retention-locked object surfaces as 409 rather than silently no-oping
    (docs/HLD.md §15), and the file lands on FAILED, not stuck in DELETING.
    Deleting from any status other than READY/FAILED is rejected by
    lifecycle_service.transition itself (docs/LLD.md §4.2's ALLOWED_TRANSITIONS
    only has DELETING reachable from those two) — no separate check needed
    here, InvalidStateTransitionError (409) already covers it.

    Removes every native S3/MinIO *version* of each key, not just a single
    version-less RemoveObject call: since bucket_bootstrap.py enables native
    bucket versioning on BUCKET_FILES (docs/LLD.md §3.2), a version-less
    `remove_object` only adds a delete marker — it never touches (and never
    triggers a retention error for) the actual locked version underneath.
    Verified directly against a real MinIO server: a version-less call on a
    COMPLIANCE-locked object silently "succeeds" (delete marker only); only
    a call naming the specific version_id actually attempts to remove the
    protected data and raises. Also verified directly: the real error code
    MinIO returns for a WORM-protected object is `InvalidRequest` ("Object
    is WORM protected and cannot be overwritten"), not the `AccessDenied`
    docs/HLD.md §15's prose names — both are treated as the retention-lock
    signal here since the observed behavior is authoritative over the doc.
    """
    file = await permission_service.assert_access(db, document_id, requester_id, PermissionLevel.OWNER)

    # Committed on its own before any MinIO call: if something below fails
    # unexpectedly (not a retention lock — see the except branch, which
    # commits its own FAILED write), this DELETING write must still survive
    # rather than being rolled back by get_db's request-transaction wrapper.
    await FileRepository.update_status(db, file, FileStatus.DELETING)
    await db.commit()

    versions = await FileVersionRepository.list_for_document(db, document_id)
    for version in versions:
        native_versions = minio_client.list_objects(settings.BUCKET_FILES, prefix=version.storage_key, include_version=True)
        for native_version in native_versions:
            try:
                minio_client.remove_object(settings.BUCKET_FILES, version.storage_key, version_id=native_version.version_id)
            except S3Error as e:
                if e.code not in ("AccessDenied", "InvalidRequest"):
                    raise
                await FileRepository.update_status(db, file, FileStatus.FAILED, last_error_code="RETENTION_LOCKED")
                await db.commit()
                raise RetentionLockedError(document_id) from e

    await FileRepository.update_status(db, file, FileStatus.DELETED, deleted_at=datetime.now(timezone.utc))
    await OutboxRepository.insert(
        db, aggregate_id=document_id, event_type="file.deleted", payload=_build_file_deleted_payload(file)
    )
    await db.commit()

    return file
