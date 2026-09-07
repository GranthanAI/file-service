from app.core.config import settings
from app.core.exceptions import InvalidContentTypeError


def _normalize(content_type: str) -> str:
    return content_type.split(";")[0].strip().lower()


def assert_allowed(content_type: str) -> None:
    """MIME allowlist enforcement (docs/LLD.md §1, §5.1) — called from
    upload_service.initiate_upload() before anything is written to
    PostgreSQL or MinIO. This is step 1 of the trust chain (declared MIME
    only); see storage/object_verifier.py for the remaining steps."""
    if _normalize(content_type) not in {ct.lower() for ct in settings.allowed_mime_types}:
        raise InvalidContentTypeError(content_type)
