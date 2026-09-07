from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.core.enums import FileStatus, PresignMethod


class InitiateUploadRequest(BaseModel):
    """docs/LLD.md §5.1."""

    conversation_id: UUID | None = None
    filename: str = Field(min_length=1, max_length=255)
    content_type: str
    size_bytes: int = Field(gt=0)

    @field_validator("filename")
    @classmethod
    def _no_path_separators(cls, value: str) -> str:
        """docs/LLD.md §5.1: "filename sanitized (no path separators)" —
        prevents a filename like "../../etc/passwd" or "a/b.pdf" from
        influencing anything (it only ever contributes its extension to
        storage.key_builder.build_storage_key, but reject early regardless)."""
        if "/" in value or "\\" in value:
            raise ValueError("filename must not contain path separators")
        return value


class InitiateUploadResponse(BaseModel):
    """docs/LLD.md §5.1."""

    document_id: UUID
    upload_url: str
    upload_method: PresignMethod
    form_data: dict[str, str] | None = None
    expires_in: int


class CompleteUploadRequest(BaseModel):
    """docs/LLD.md §5.2 — empty body is also valid (client_checksum optional)."""

    client_checksum: str | None = None


class CompleteUploadResponse(BaseModel):
    """docs/LLD.md §5.2."""

    document_id: UUID
    status: FileStatus
    etag: str | None
    checksum_sha256: str | None
    size_bytes: int
    version: int
