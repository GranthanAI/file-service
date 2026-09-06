from enum import Enum


class FileStatus(str, Enum):
    """See docs/LLD.md §2.1 for the full state machine and why SCANNING
    exists as a separate state from UPLOADED/PROCESSING."""

    UPLOADING = "UPLOADING"
    UPLOADED = "UPLOADED"
    SCANNING = "SCANNING"
    PROCESSING = "PROCESSING"
    READY = "READY"
    FAILED = "FAILED"
    DELETING = "DELETING"
    DELETED = "DELETED"


class PermissionLevel(str, Enum):
    VIEWER = "VIEWER"
    EDITOR = "EDITOR"
    OWNER = "OWNER"


class PrincipalType(str, Enum):
    """USER only is implemented today; TEAM is a reserved, forward-compatible
    value only (see docs/LLD.md §2.4) — not a Postgres ENUM type, a plain
    VARCHAR, so adding TEAM later needs no migration."""

    USER = "USER"
    TEAM = "TEAM"


class UploadJobStatus(str, Enum):
    """Plain VARCHAR on upload_jobs.status, not a Postgres ENUM type
    (see docs/LLD.md §2.2)."""

    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    EXPIRED = "EXPIRED"


class PresignMethod(str, Enum):
    PUT = "PUT"
    POST = "POST"
