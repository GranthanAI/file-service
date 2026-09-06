from app.core.enums import FileStatus


class FileServiceError(Exception):
    """Base class for domain exceptions. Later phases (see
    docs/IMPLEMENTATION_PLAN.md Phase 3) add the rest of the set from
    docs/LLD.md §8 (InvalidContentTypeError, UploadSizeExceededError,
    PermissionDeniedError, ObjectNotFoundError, SizeMismatchError,
    ContentTypeMismatchError, etc.) plus the central FastAPI exception
    handler registration in app/main.py."""


class InvalidStateTransitionError(FileServiceError):
    """Raised by lifecycle_service.transition() on an illegal file_status
    edge (docs/LLD.md §4.2)."""

    def __init__(self, current: FileStatus, target: FileStatus) -> None:
        self.current = current
        self.target = target
        super().__init__(f"Cannot transition file status from {current.value} to {target.value}")
