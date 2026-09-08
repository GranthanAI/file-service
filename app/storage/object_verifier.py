from dataclasses import dataclass

from minio import Minio
from minio.error import S3Error

from app.core.exceptions import ContentTypeMismatchError, ObjectNotFoundError, SizeMismatchError
from app.core.metrics import minio_stat_object_duration_seconds


@dataclass(frozen=True)
class ObjectVerification:
    etag: str
    size: int
    content_type: str


def _normalize_content_type(content_type: str) -> str:
    """Strip parameters (e.g. "; charset=utf-8") and normalize case/
    whitespace — exact match only, never prefix/substring matching (a
    prefix check would let "application/pdf-malicious" pass against an
    expected "application/pdf"). See docs/LLD.md §3.5."""
    return content_type.split(";")[0].strip().lower()


def verify_uploaded_object(
    client: Minio,
    bucket: str,
    key: str,
    expected_size: int,
    expected_content_type: str,
) -> ObjectVerification:
    """See docs/LLD.md §3.5 — the first trust boundary: stop believing
    client-declared metadata, start believing MinIO's own record of what
    bytes actually landed. Called exclusively from
    upload_service.complete_upload().

    This only proves the declared content-type matches what MinIO recorded
    as the HTTP Content-Type on upload — that header is still client-
    supplied at PUT time, not proof of the file's real format. Magic-byte
    sniffing, file-parser validation, and the AV scan are the remaining
    links in the trust chain, deliberately NOT performed here (StatObject
    only reads metadata, never the object body) — see docs/LLD.md §3.5/§4.5.
    """
    try:
        with minio_stat_object_duration_seconds.time():
            stat = client.stat_object(bucket, key)
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchObject"):
            raise ObjectNotFoundError(bucket, key) from e
        raise

    if stat.size != expected_size:
        raise SizeMismatchError(expected_size, stat.size)

    actual_ct = _normalize_content_type(stat.content_type or "")
    expected_ct = _normalize_content_type(expected_content_type)
    if actual_ct != expected_ct:
        raise ContentTypeMismatchError(expected_ct, actual_ct)

    return ObjectVerification(etag=stat.etag, size=stat.size, content_type=actual_ct)
