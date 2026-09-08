from uuid import UUID

from fastapi import APIRouter, Depends
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user_id, get_db
from app.api.schemas.download import DownloadResponse
from app.core.metrics import download_requests_total
from app.services import download_service
from app.storage.minio_client import get_minio_client

router = APIRouter(tags=["download"])


@router.get("/files/{document_id}/download", response_model=DownloadResponse)
async def download(
    document_id: UUID,
    requester_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    minio_client: Minio = Depends(get_minio_client),
) -> DownloadResponse:
    """docs/LLD.md §5.5/§6.4."""
    download_requests_total.inc()
    download_url, expires_in = await download_service.get_download_url(db, minio_client, document_id, requester_id)
    return DownloadResponse(download_url=download_url, expires_in=expires_in)
