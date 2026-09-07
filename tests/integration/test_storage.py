"""Phase 2 exit-criteria tests (docs/IMPLEMENTATION_PLAN.md): presign a PUT
(and a POST policy), upload a small file via httpx, confirm StatObject
verification succeeds, and confirm bucket_bootstrap is idempotent."""

from datetime import timedelta

import httpx
import pytest
from minio import Minio

from app.core.exceptions import ContentTypeMismatchError, ObjectNotFoundError, SizeMismatchError
from app.storage.bucket_bootstrap import bootstrap_buckets
from app.storage.object_verifier import verify_uploaded_object
from app.storage.presign import presign_get, presign_post_policy, presign_put

CONTENT = b"%PDF-1.4 fake pdf content for testing"


def test_presign_put_upload_and_verify(minio_client: Minio, minio_test_bucket: str) -> None:
    key = "owner_1/doc_1/v1/original.pdf"
    url = presign_put(minio_client, minio_test_bucket, key, expiry=timedelta(minutes=15))

    response = httpx.put(url, content=CONTENT, headers={"Content-Type": "application/pdf"})
    assert response.status_code == 200

    verification = verify_uploaded_object(
        minio_client, minio_test_bucket, key, expected_size=len(CONTENT), expected_content_type="application/pdf"
    )
    assert verification.size == len(CONTENT)
    assert verification.content_type == "application/pdf"
    assert verification.etag


def test_presign_post_policy_upload_and_verify(minio_client: Minio, minio_test_bucket: str) -> None:
    key = "owner_2/doc_2/v1/original.pdf"
    upload_url, form_data = presign_post_policy(
        minio_client,
        minio_test_bucket,
        key,
        content_type="application/pdf",
        max_size=1024,
        expiry=timedelta(minutes=15),
    )

    files = {"file": (key, CONTENT)}
    response = httpx.post(upload_url, data=form_data, files=files)
    assert response.status_code in (200, 204)

    verification = verify_uploaded_object(
        minio_client, minio_test_bucket, key, expected_size=len(CONTENT), expected_content_type="application/pdf"
    )
    assert verification.size == len(CONTENT)


def test_presign_get_downloads_same_bytes(minio_client: Minio, minio_test_bucket: str) -> None:
    key = "owner_3/doc_3/v1/original.pdf"
    put_url = presign_put(minio_client, minio_test_bucket, key, expiry=timedelta(minutes=15))
    httpx.put(put_url, content=CONTENT, headers={"Content-Type": "application/pdf"})

    get_url = presign_get(minio_client, minio_test_bucket, key, expiry=timedelta(minutes=5))
    response = httpx.get(get_url)
    assert response.status_code == 200
    assert response.content == CONTENT


def test_verify_uploaded_object_missing_key_raises(minio_client: Minio, minio_test_bucket: str) -> None:
    with pytest.raises(ObjectNotFoundError):
        verify_uploaded_object(
            minio_client, minio_test_bucket, "does/not/exist.pdf", expected_size=10, expected_content_type="application/pdf"
        )


def test_verify_uploaded_object_size_mismatch_raises(minio_client: Minio, minio_test_bucket: str) -> None:
    key = "owner_4/doc_4/v1/original.pdf"
    url = presign_put(minio_client, minio_test_bucket, key, expiry=timedelta(minutes=15))
    httpx.put(url, content=CONTENT, headers={"Content-Type": "application/pdf"})

    with pytest.raises(SizeMismatchError):
        verify_uploaded_object(
            minio_client, minio_test_bucket, key, expected_size=len(CONTENT) + 1, expected_content_type="application/pdf"
        )


def test_verify_uploaded_object_content_type_mismatch_raises(minio_client: Minio, minio_test_bucket: str) -> None:
    key = "owner_5/doc_5/v1/original.pdf"
    url = presign_put(minio_client, minio_test_bucket, key, expiry=timedelta(minutes=15))
    httpx.put(url, content=CONTENT, headers={"Content-Type": "application/pdf"})

    with pytest.raises(ContentTypeMismatchError):
        verify_uploaded_object(
            minio_client, minio_test_bucket, key, expected_size=len(CONTENT), expected_content_type="text/plain"
        )


def test_bucket_bootstrap_is_idempotent(minio_client: Minio) -> None:
    bootstrap_buckets(minio_client)
    bootstrap_buckets(minio_client)  # must not raise the second time

    from app.core.config import settings

    assert minio_client.bucket_exists(settings.BUCKET_FILES)
    assert minio_client.bucket_exists(settings.BUCKET_QUARANTINE)
