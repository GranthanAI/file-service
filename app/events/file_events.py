"""Pydantic payload models for every event File Service produces.

docs/LLD.md §1 describes this module as used "both to build outbox payloads
and in contract tests" (Phase 12). Concretely: `EVENT_MODELS` lets
outbox_publisher.py validate + serialize each outbox row through the model
matching its event_type right before publishing, so a malformed row fails
loudly here instead of putting bad JSON on the topic — and the same models
are the natural fixture for the snapshot/contract tests docs/LLD.md §10
calls for.

`event_id` / `event_type` / `version` are deliberately NOT part of what
services write into `outbox_events.payload` (see
upload_service._build_file_uploaded_payload's docstring) — they're stamped
on by outbox_publisher.py from the outbox_events row itself at publish time
(row.id -> event_id, row.event_type -> event_type, a fixed schema version).
Every model below declares them so the fully-assembled wire message
validates, but the service-side payload builders never set them.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel


class StorageRef(BaseModel):
    """docs/HLD.md §13 — a durable storage reference, not a presigned URL
    (those expire; this doesn't)."""

    provider: str
    bucket: str
    key: str
    etag: str


class FileUploadedEvent(BaseModel):
    """docs/HLD.md §13. The only downstream consumer today is Retrieval
    Service (docs/LLD.md §3.6) — parse -> chunk -> embed -> Milvus."""

    event_id: UUID
    event_type: Literal["file.uploaded"] = "file.uploaded"
    version: int = 1
    document_id: UUID
    conversation_id: UUID | None
    owner_id: UUID
    content_version: int | None = None
    filename: str
    content_type: str
    storage: StorageRef
    timestamp: datetime


class FileDeletedEvent(BaseModel):
    """docs/LLD.md §6.5 — consumed by downstream services to clean up
    indexed representations (vector store, chunks, etc.) on delete."""

    event_id: UUID
    event_type: Literal["file.deleted"] = "file.deleted"
    version: int = 1
    document_id: UUID
    owner_id: UUID
    content_version: int | None = None
    timestamp: datetime


class FileProcessingRetryEvent(BaseModel):
    """docs/LLD.md §4.4 retry_service — the exact payload shape shown
    there; re-signals Retrieval Service to re-attempt processing without
    touching the original object in MinIO."""

    event_id: UUID
    event_type: Literal["file.processing_retry"] = "file.processing_retry"
    version: int = 1
    document_id: UUID
    content_version: int
    retry_count: int
    timestamp: datetime


class FileScanFailedEvent(BaseModel):
    """docs/LLD.md §4.5 quarantine flow — emitted instead of file.uploaded
    when the AV scan finds an infected object; file.uploaded is never
    emitted for the same document_id."""

    event_id: UUID
    event_type: Literal["file.scan_failed"] = "file.scan_failed"
    version: int = 1
    document_id: UUID
    error_code: str
    timestamp: datetime


# Keyed by the same event_type string stored in outbox_events.event_type —
# see app/events/topics.py for the topic-name side of that same mapping.
EVENT_MODELS: dict[str, type[BaseModel]] = {
    "file.uploaded": FileUploadedEvent,
    "file.deleted": FileDeletedEvent,
    "file.processing_retry": FileProcessingRetryEvent,
    "file.scan_failed": FileScanFailedEvent,
}
