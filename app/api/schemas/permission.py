from uuid import UUID

from pydantic import BaseModel, Field

from app.core.enums import PermissionLevel, PrincipalType


class GrantPermissionRequest(BaseModel):
    """docs/LLD.md §5.9."""

    principal_id: UUID  # the user being granted access — not the caller
    # Defaults to USER since that's the only value the app writes/reads today
    # (docs/LLD.md §2.4) — accepting the field at all keeps the request shape
    # forward-compatible with TEAM without a breaking schema change later.
    principal_type: PrincipalType = PrincipalType.USER
    access_level: PermissionLevel


class PermissionResponse(BaseModel):
    """docs/LLD.md §5.9 — echoes the created/updated grant."""

    document_id: UUID
    principal_id: UUID
    principal_type: PrincipalType
    access_level: PermissionLevel
    granted_by: UUID


class PermissionCheckRequest(BaseModel):
    """Retrieval HLD 16.2 - batched, internal-only."""

    user_id: UUID
    document_ids: list[UUID] = Field(max_length=500)


class PermissionCheckResponse(BaseModel):
    authorized_document_ids: list[UUID]
