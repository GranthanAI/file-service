from uuid import UUID

from fastapi import APIRouter, Depends, status
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user_id, get_db
from app.api.schemas.upload import (
    CompleteUploadRequest,
    CompleteUploadResponse,
    InitiateUploadRequest,
    InitiateUploadResponse,
)
from app.services import upload_service
from app.storage.minio_client import get_minio_client

router = APIRouter(tags=["upload"])


@router.post("/files/upload/initiate", response_model=InitiateUploadResponse, status_code=status.HTTP_201_CREATED)
async def initiate_upload(
    body: InitiateUploadRequest,
    owner_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    minio_client: Minio = Depends(get_minio_client),
) -> InitiateUploadResponse:
    """docs/LLD.md §5.1."""
    result = await upload_service.initiate_upload(
        db,
        minio_client,
        owner_id=owner_id,
        conversation_id=body.conversation_id,
        filename=body.filename,
        content_type=body.content_type,
        size_bytes=body.size_bytes,
    )
    return InitiateUploadResponse(
        document_id=result.document_id,
        upload_url=result.upload_url,
        upload_method=result.upload_method,
        form_data=result.form_data,
        expires_in=result.expires_in,
    )


@router.post("/files/{document_id}/complete", response_model=CompleteUploadResponse)
async def complete_upload(
    document_id: UUID,
    body: CompleteUploadRequest = CompleteUploadRequest(),
    requester_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    minio_client: Minio = Depends(get_minio_client),
) -> CompleteUploadResponse:
    """docs/LLD.md §5.2."""
    file = await upload_service.complete_upload(
        db,
        minio_client,
        document_id=document_id,
        requester_id=requester_id,
        client_checksum=body.client_checksum,
    )
    return CompleteUploadResponse(
        document_id=file.document_id,
        status=file.status,
        etag=file.etag,
        checksum_sha256=file.checksum_sha256,
        size_bytes=file.size_bytes,
        version=file.current_version,
    )
