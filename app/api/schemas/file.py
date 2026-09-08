from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.core.enums import FileStatus


class FileResponse(BaseModel):
    """docs/LLD.md §5.3."""

    document_id: UUID
    conversation_id: UUID | None
    owner_id: UUID
    filename: str
    content_type: str
    size_bytes: int
    status: FileStatus
    current_version: int
    created_at: datetime
    updated_at: datetime


class FileListItem(BaseModel):
    """docs/LLD.md §5.4 — a lighter projection than FileResponse, matching the
    documented list-item payload exactly (document_id, filename, status)."""

    document_id: UUID
    filename: str
    status: FileStatus


class FileListResponse(BaseModel):
    """docs/LLD.md §5.4."""

    items: list[FileListItem]
    # None means "this was the last page" — the client stops paging when it
    # sees null rather than needing to compare page sizes itself.
    next_cursor: UUID | None = None


class FileStatusResponse(BaseModel):
    """docs/LLD.md §5.6."""

    document_id: UUID
    status: FileStatus
    retry_count: int
