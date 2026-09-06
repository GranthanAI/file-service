import uuid
from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Enum as SQLEnum, ForeignKey, Index, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.enums import PermissionLevel, PrincipalType
from app.db.base import Base


class FilePermission(Base):
    """See docs/LLD.md §2.4. USER-only scope for now — principal_type is a
    plain VARCHAR (not a Postgres ENUM) so adding TEAM later needs no
    migration, but only PrincipalType.USER is written/read today."""

    __tablename__ = "file_permissions"
    __table_args__ = (
        UniqueConstraint(
            "document_id", "principal_id", "principal_type", name="uq_file_permissions_document_principal"
        ),
        Index("idx_file_permissions_principal", "principal_id", "principal_type"),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("files.document_id", ondelete="CASCADE"), nullable=False
    )
    principal_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    principal_type: Mapped[str] = mapped_column(
        String(10), nullable=False, default=PrincipalType.USER.value, server_default=PrincipalType.USER.value
    )
    access_level: Mapped[PermissionLevel] = mapped_column(SQLEnum(PermissionLevel, name="permission_level"), nullable=False)
    granted_by: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
