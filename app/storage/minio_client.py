from minio import Minio

from app.core.config import settings

_client: Minio | None = None


def get_minio_client() -> Minio:
    """Process-scoped reusable MinIO client (see docs/LLD.md §3.1).

    Not a global singleton across processes: each Uvicorn/Gunicorn worker
    process constructs and holds its own instance, which is expected and
    cheap. Within one worker process this same instance is reused across
    requests.
    """
    global _client
    if _client is None:
        _client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_USE_TLS,
        )
    return _client
