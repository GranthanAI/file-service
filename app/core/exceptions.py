from app.core.enums import FileStatus


class FileServiceError(Exception):
    """Base class for domain exceptions. Phase 3 adds the remaining set from
    docs/LLD.md §8 (PermissionDeniedError, AlreadyCompletedError,
    PresignExpiredError, UploadRateLimitExceededError, MinioUnavailableError)
    plus the central FastAPI exception handler registration in app/main.py."""


class InvalidStateTransitionError(FileServiceError):
    """Raised by lifecycle_service.transition() on an illegal file_status
    edge (docs/LLD.md §4.2)."""

    def __init__(self, current: FileStatus, target: FileStatus) -> None:
        self.current = current
        self.target = target
        super().__init__(f"Cannot transition file status from {current.value} to {target.value}")


# --- Pulled forward from Phase 3 (docs/LLD.md §8) because Phase 2's own
# storage/validation modules raise them directly: ---


class InvalidContentTypeError(FileServiceError):
    """Raised by validation/content_type.py — declared content_type is not
    in the allowlist (docs/LLD.md §5.1). Maps to HTTP 400."""

    def __init__(self, content_type: str) -> None:
        self.content_type = content_type
        super().__init__(f"Content type '{content_type}' is not allowed")


class UploadSizeExceededError(FileServiceError):
    """Raised by validation/size_limits.py — declared size_bytes exceeds
    MAX_UPLOAD_SIZE_BYTES (docs/LLD.md §5.1). Maps to HTTP 400."""

    def __init__(self, size_bytes: int, max_allowed: int) -> None:
        self.size_bytes = size_bytes
        self.max_allowed = max_allowed
        super().__init__(f"Upload size {size_bytes} exceeds the maximum allowed size of {max_allowed} bytes")


class ObjectNotFoundError(FileServiceError):
    """Raised by storage/object_verifier.py — StatObject found nothing at
    /complete time (docs/LLD.md §3.5, §9). Maps to HTTP 422."""

    def __init__(self, bucket: str, key: str) -> None:
        self.bucket = bucket
        self.key = key
        super().__init__(f"Object not found: {bucket}/{key}")


class SizeMismatchError(FileServiceError):
    """Raised by storage/object_verifier.py — uploaded object size does not
    match the size declared at initiate (docs/LLD.md §3.5). Maps to HTTP 422."""

    def __init__(self, expected_size: int, actual_size: int) -> None:
        self.expected_size = expected_size
        self.actual_size = actual_size
        super().__init__(f"Expected size {expected_size} bytes but object is {actual_size} bytes")


class ContentTypeMismatchError(FileServiceError):
    """Raised by storage/object_verifier.py — MinIO's recorded Content-Type
    does not exactly match what was declared at initiate (docs/LLD.md §3.5:
    exact normalized comparison, never a prefix/substring check). Maps to
    HTTP 422."""

    def __init__(self, expected_content_type: str, actual_content_type: str) -> None:
        self.expected_content_type = expected_content_type
        self.actual_content_type = actual_content_type
        super().__init__(
            f"Expected content type '{expected_content_type}' but object has '{actual_content_type}'"
        )
