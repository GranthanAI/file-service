from app.core.config import settings
from app.core.exceptions import UploadSizeExceededError


def assert_within_limit(size_bytes: int) -> None:
    """Global upload cap enforcement (docs/LLD.md §1, §5.1) — called from
    upload_service.initiate_upload(). Per-plan/per-owner limits are a
    forward-compatible extension point, not implemented today (no product
    requirement for tiered limits yet — see the USER-only permission scope
    decision in docs/LLD.md §2.4 for the same "don't add complexity the
    product doesn't need yet" reasoning)."""
    if size_bytes <= 0:
        raise UploadSizeExceededError(size_bytes, settings.MAX_UPLOAD_SIZE_BYTES)
    if size_bytes > settings.MAX_UPLOAD_SIZE_BYTES:
        raise UploadSizeExceededError(size_bytes, settings.MAX_UPLOAD_SIZE_BYTES)
