from datetime import timedelta
from uuid import UUID

from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import PermissionLevel
from app.services import permission_service
from app.storage.presign import presign_get


async def get_download_url(db: AsyncSession, minio_client: Minio, document_id: UUID, requester_id: UUID) -> tuple[str, int]:
    """docs/LLD.md §6.4/§5.5 — VIEWER+ access required (owner implicitly
    qualifies, see permission_service.assert_access). Returns (download_url,
    expires_in) rather than a schema object so this stays decoupled from the
    API layer, matching file_service.get_file's shape."""
    file = await permission_service.assert_access(db, document_id, requester_id, PermissionLevel.VIEWER)
    ttl = timedelta(seconds=settings.DOWNLOAD_PRESIGN_TTL_SECONDS)
    url = presign_get(minio_client, file.storage_bucket, file.storage_key, expiry=ttl, response_filename=file.filename)
    return url, settings.DOWNLOAD_PRESIGN_TTL_SECONDS
