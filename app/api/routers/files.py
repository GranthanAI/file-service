from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user_id, get_db
from app.api.schemas.file import DeleteResponse, FileListItem, FileListResponse, FileResponse
from app.core.enums import FileStatus
from app.core.metrics import delete_requests_total
from app.services import delete_service, file_service
from app.storage.minio_client import get_minio_client

router = APIRouter(tags=["files"])


@router.get("/files/{document_id}", response_model=FileResponse)
async def get_file(
    document_id: UUID,
    requester_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
) -> FileResponse:
    """docs/LLD.md §5.3."""
    # file_service.get_file runs the permission check internally (VIEWER+)
    # and raises FileRecordNotFoundError/PermissionDeniedError, which the
    # handlers in app/core/exceptions.py turn into 404/403 automatically —
    # nothing else to do here on the failure path.
    file = await file_service.get_file(db, document_id, requester_id)
    # Explicit field-by-field construction (not .model_validate on the ORM
    # object) — keeps the wire schema decoupled from the DB model shape.
    return FileResponse(
        document_id=file.document_id,
        conversation_id=file.conversation_id,
        owner_id=file.owner_id,
        filename=file.filename,
        content_type=file.content_type,
        size_bytes=file.size_bytes,
        status=file.status,
        current_version=file.current_version,
        created_at=file.created_at,
        updated_at=file.updated_at,
    )


@router.get("/files", response_model=FileListResponse)
async def list_files(
    conversation_id: UUID | None = None,
    status: FileStatus | None = None,
    cursor: UUID | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    requester_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
) -> FileListResponse:
    """docs/LLD.md §5.4 — keyset pagination; next_cursor is the last item's
    document_id when a full page came back (a likely-more-rows signal), null
    on a short/empty page."""
    # No explicit permission check here: file_service.list_files always
    # scopes the query to owner_id=requester_id, so there is nothing for a
    # stranger/viewer to see or leak — the filter itself is the guard.
    files = await file_service.list_files(
        db,
        requester_id=requester_id,
        conversation_id=conversation_id,
        status=status,
        cursor=cursor,
        limit=limit,
    )
    # A full page (len == limit) means there may be more rows past it, so we
    # hand back its last id as the next cursor; a short page is the last one.
    next_cursor = files[-1].document_id if len(files) == limit else None
    return FileListResponse(
        items=[FileListItem(document_id=f.document_id, filename=f.filename, status=f.status) for f in files],
        next_cursor=next_cursor,
    )


@router.delete("/files/{document_id}", response_model=DeleteResponse, status_code=status.HTTP_202_ACCEPTED)
async def delete_file(
    document_id: UUID,
    requester_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    minio_client: Minio = Depends(get_minio_client),
) -> DeleteResponse:
    """docs/LLD.md §5.8/§6.5 — requires OWNER."""
    delete_requests_total.inc()
    file = await delete_service.delete_file(db, minio_client, document_id, requester_id)
    return DeleteResponse(document_id=file.document_id, status=file.status)
