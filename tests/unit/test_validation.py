import pytest

from app.core.exceptions import InvalidContentTypeError, UploadSizeExceededError
from app.validation import content_type, size_limits


class TestContentTypeValidation:
    def test_allowed_content_type_passes(self) -> None:
        content_type.assert_allowed("application/pdf")  # should not raise

    def test_allowed_content_type_is_case_insensitive(self) -> None:
        content_type.assert_allowed("APPLICATION/PDF")  # should not raise

    def test_disallowed_content_type_raises(self) -> None:
        with pytest.raises(InvalidContentTypeError):
            content_type.assert_allowed("application/x-msdownload")

    def test_prefix_spoofed_content_type_is_rejected(self) -> None:
        """Same exact-match principle as object_verifier.py (docs/LLD.md §3.5)."""
        with pytest.raises(InvalidContentTypeError):
            content_type.assert_allowed("application/pdf-malicious")


class TestSizeLimitValidation:
    def test_within_limit_passes(self) -> None:
        size_limits.assert_within_limit(1024)  # should not raise

    def test_at_exact_limit_passes(self) -> None:
        from app.core.config import settings

        size_limits.assert_within_limit(settings.MAX_UPLOAD_SIZE_BYTES)  # should not raise

    def test_over_limit_raises(self) -> None:
        from app.core.config import settings

        with pytest.raises(UploadSizeExceededError):
            size_limits.assert_within_limit(settings.MAX_UPLOAD_SIZE_BYTES + 1)

    def test_zero_size_raises(self) -> None:
        with pytest.raises(UploadSizeExceededError):
            size_limits.assert_within_limit(0)

    def test_negative_size_raises(self) -> None:
        with pytest.raises(UploadSizeExceededError):
            size_limits.assert_within_limit(-1)
