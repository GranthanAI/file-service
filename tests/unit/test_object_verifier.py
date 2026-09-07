from unittest.mock import MagicMock

import pytest
from minio.error import S3Error

from app.core.exceptions import ContentTypeMismatchError, ObjectNotFoundError, SizeMismatchError
from app.storage.object_verifier import _normalize_content_type, verify_uploaded_object


def _fake_s3_not_found_error() -> S3Error:
    return S3Error(
        response=None,
        code="NoSuchKey",
        message="Object does not exist",
        resource="/bucket/key",
        request_id="req-1",
        host_id="host-1",
    )


def _stat_result(size: int, etag: str, content_type: str) -> MagicMock:
    stat = MagicMock()
    stat.size = size
    stat.etag = etag
    stat.content_type = content_type
    return stat


class TestNormalizeContentType:
    def test_strips_parameters(self) -> None:
        assert _normalize_content_type("text/plain; charset=utf-8") == "text/plain"

    def test_lowercases(self) -> None:
        assert _normalize_content_type("APPLICATION/PDF") == "application/pdf"

    def test_strips_whitespace(self) -> None:
        assert _normalize_content_type("  application/pdf  ") == "application/pdf"

    def test_rejects_similarly_prefixed_type(self) -> None:
        """Regression test for the exact-match fix (docs/LLD.md §3.5): a
        prefix/substring check would incorrectly accept this."""
        assert _normalize_content_type("application/pdf-malicious") != _normalize_content_type("application/pdf")


class TestVerifyUploadedObject:
    def test_success_returns_verification(self) -> None:
        client = MagicMock()
        client.stat_object.return_value = _stat_result(1024, "abc123", "application/pdf")

        result = verify_uploaded_object(client, "bucket", "key", expected_size=1024, expected_content_type="application/pdf")

        assert result.size == 1024
        assert result.etag == "abc123"
        assert result.content_type == "application/pdf"

    def test_content_type_with_parameters_still_matches_exactly(self) -> None:
        client = MagicMock()
        client.stat_object.return_value = _stat_result(10, "etag", "application/pdf; charset=binary")

        result = verify_uploaded_object(client, "bucket", "key", expected_size=10, expected_content_type="application/pdf")

        assert result.content_type == "application/pdf"

    def test_missing_object_raises_object_not_found(self) -> None:
        client = MagicMock()
        client.stat_object.side_effect = _fake_s3_not_found_error()

        with pytest.raises(ObjectNotFoundError):
            verify_uploaded_object(client, "bucket", "key", expected_size=10, expected_content_type="application/pdf")

    def test_size_mismatch_raises(self) -> None:
        client = MagicMock()
        client.stat_object.return_value = _stat_result(999, "etag", "application/pdf")

        with pytest.raises(SizeMismatchError):
            verify_uploaded_object(client, "bucket", "key", expected_size=1024, expected_content_type="application/pdf")

    def test_content_type_mismatch_raises(self) -> None:
        client = MagicMock()
        client.stat_object.return_value = _stat_result(10, "etag", "text/plain")

        with pytest.raises(ContentTypeMismatchError):
            verify_uploaded_object(client, "bucket", "key", expected_size=10, expected_content_type="application/pdf")

    def test_prefix_spoofed_content_type_does_not_pass(self) -> None:
        """The core regression test the design review called out: a stored
        Content-Type of 'application/pdf-malicious' must NOT satisfy an
        expectation of 'application/pdf' via prefix matching."""
        client = MagicMock()
        client.stat_object.return_value = _stat_result(10, "etag", "application/pdf-malicious")

        with pytest.raises(ContentTypeMismatchError):
            verify_uploaded_object(client, "bucket", "key", expected_size=10, expected_content_type="application/pdf")
