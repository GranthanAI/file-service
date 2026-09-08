from uuid import UUID

from app.core.enums import FileStatus


class FileServiceError(Exception):
    """Base class for domain exceptions. Later phases add the remaining set
    from docs/LLD.md §8 (RetentionLockedError — Phase 8 delete;
    UploadRateLimitExceededError, MinioUnavailableError — not yet needed by
    any implemented code path)."""


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


# --- Phase 3 additions (docs/LLD.md §8) ---


class PermissionDeniedError(FileServiceError):
    """Raised when the requester is not the file's owner (docs/LLD.md §4.1
    step 2, §9). Maps to HTTP 403."""

    def __init__(self, document_id: UUID, requester_id: UUID) -> None:
        self.document_id = document_id
        self.requester_id = requester_id
        super().__init__(f"User {requester_id} is not permitted to access document {document_id}")


class FileRecordNotFoundError(FileServiceError):
    """Raised when `document_id` has no corresponding `files` row
    (docs/LLD.md §5.2). Named to avoid shadowing Python's builtin
    `FileNotFoundError` (an OS-level exception) — this is a DB-record lookup
    failure, a different thing entirely. Maps to HTTP 404."""

    def __init__(self, document_id: UUID) -> None:
        self.document_id = document_id
        super().__init__(f"No file found with document_id {document_id}")


class AlreadyCompletedError(FileServiceError):
    """Raised when `/complete` is called again for an already-completed
    upload but with different terms than what was actually completed (e.g. a
    mismatched client_checksum) — docs/LLD.md §5.2. A repeat call with
    matching/no terms is idempotent (200), not this. Maps to HTTP 409."""

    def __init__(self, document_id: UUID) -> None:
        self.document_id = document_id
        super().__init__(f"Document {document_id} was already completed with different terms")


class PresignExpiredError(FileServiceError):
    """Raised when `/complete` is called after the upload_jobs presign
    window has passed (docs/LLD.md §4.1 step 5) — caller must re-initiate.
    Maps to HTTP 410."""

    def __init__(self, document_id: UUID) -> None:
        self.document_id = document_id
        super().__init__(f"Presigned upload for document {document_id} has expired; re-initiate the upload")


# --- Phase 7 addition (docs/LLD.md §4.4, §5.7) ---


class RetryNotAllowedError(FileServiceError):
    """Raised by retry_service.retry_upload — retry is only valid from
    FAILED (docs/LLD.md §4.4/§5.7). The lifecycle_service state machine
    itself would actually permit some other current states to reach
    PROCESSING (e.g. READY -> PROCESSING for a legitimate reindex,
    docs/LLD.md §4.2) — this rule is specific to the /retry endpoint's
    business precondition, not a state-machine edge violation, so it's
    enforced here rather than by lifecycle_service.transition. Maps to
    HTTP 409."""

    def __init__(self, document_id: UUID, current_status: FileStatus) -> None:
        self.document_id = document_id
        self.current_status = current_status
        super().__init__(f"Cannot retry document {document_id}: current status is {current_status.value}, not FAILED")


# --- Exception -> HTTP status mapping (docs/LLD.md §8) and central handler
# registration, called once from app/main.py. Only exceptions actually
# raised by implemented code are mapped here; add an entry the same phase
# the exception is introduced. ---

EXCEPTION_STATUS_MAP: dict[type[FileServiceError], int] = {
    InvalidContentTypeError: 400,
    UploadSizeExceededError: 400,
    PermissionDeniedError: 403,
    FileRecordNotFoundError: 404,
    AlreadyCompletedError: 409,
    InvalidStateTransitionError: 409,
    RetryNotAllowedError: 409,
    PresignExpiredError: 410,
    ObjectNotFoundError: 422,
    SizeMismatchError: 422,
    ContentTypeMismatchError: 422,
}


def register_exception_handlers(app) -> None:  # noqa: ANN001 — FastAPI, avoided as an import-time type here
    from fastapi import Request
    from fastapi.responses import JSONResponse

    from app.api.schemas.common import ErrorResponse

    async def _handle(request: Request, exc: FileServiceError) -> JSONResponse:
        status_code = EXCEPTION_STATUS_MAP.get(type(exc), 500)
        request_id = getattr(request.state, "request_id", None)
        body = ErrorResponse(error_code=type(exc).__name__, message=str(exc), request_id=request_id)
        return JSONResponse(status_code=status_code, content=body.model_dump())

    for exc_class in EXCEPTION_STATUS_MAP:
        app.add_exception_handler(exc_class, _handle)
