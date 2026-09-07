from pathlib import PurePosixPath
from uuid import UUID


def build_storage_key(owner_id: UUID, document_id: UUID, version: int, filename: str) -> str:
    """Deterministic MinIO object key (docs/LLD.md §3.3, docs/HLD.md §6.2):
    `{owner_id}/{document_id}/v{version}/original.{ext}`.

    Pure function, fully unit-testable without any MinIO/DB dependency —
    this is why it lives in its own module rather than inline in
    upload_service.py.
    """
    ext = PurePosixPath(filename).suffix.lstrip(".") or "bin"
    return f"{owner_id}/{document_id}/v{version}/original.{ext}"
