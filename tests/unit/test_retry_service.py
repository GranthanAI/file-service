import uuid
from unittest.mock import AsyncMock, patch

import pytest

from app.core.enums import FileStatus
from app.core.exceptions import RetryNotAllowedError
from app.services.retry_service import retry_upload


def _fake_file(status: FileStatus, retry_count: int = 0):
    file_obj = AsyncMock()
    file_obj.document_id = uuid.uuid4()
    file_obj.status = status
    file_obj.retry_count = retry_count
    file_obj.current_version = 3
    return file_obj


class TestRetryUpload:
    async def test_failed_file_transitions_to_processing_and_publishes(self) -> None:
        file_obj = _fake_file(FileStatus.FAILED, retry_count=1)

        with (
            patch("app.services.retry_service.permission_service.assert_access", AsyncMock(return_value=file_obj)),
            patch("app.services.retry_service.FileRepository.update_status", AsyncMock()) as update_mock,
            patch("app.services.retry_service.OutboxRepository.insert", AsyncMock()) as insert_mock,
            patch("app.services.retry_service.FileRepository.increment_retry_count", AsyncMock()) as increment_mock,
        ):
            result = await retry_upload(AsyncMock(), file_obj.document_id, uuid.uuid4())

        update_mock.assert_awaited_once()
        assert update_mock.call_args.args[2] == FileStatus.PROCESSING

        insert_mock.assert_awaited_once()
        assert insert_mock.call_args.kwargs["event_type"] == "file.processing_retry"
        assert insert_mock.call_args.kwargs["payload"]["retry_count"] == 2  # old (1) + 1
        assert insert_mock.call_args.kwargs["payload"]["content_version"] == 3

        increment_mock.assert_awaited_once()
        assert result is file_obj

    async def test_non_failed_file_raises_retry_not_allowed(self) -> None:
        file_obj = _fake_file(FileStatus.READY)

        with patch("app.services.retry_service.permission_service.assert_access", AsyncMock(return_value=file_obj)):
            with pytest.raises(RetryNotAllowedError):
                await retry_upload(AsyncMock(), file_obj.document_id, uuid.uuid4())

    async def test_insert_and_increment_are_never_called_when_not_allowed(self) -> None:
        file_obj = _fake_file(FileStatus.UPLOADING)

        with (
            patch("app.services.retry_service.permission_service.assert_access", AsyncMock(return_value=file_obj)),
            patch("app.services.retry_service.OutboxRepository.insert", AsyncMock()) as insert_mock,
            patch("app.services.retry_service.FileRepository.increment_retry_count", AsyncMock()) as increment_mock,
        ):
            with pytest.raises(RetryNotAllowedError):
                await retry_upload(AsyncMock(), file_obj.document_id, uuid.uuid4())

        insert_mock.assert_not_called()
        increment_mock.assert_not_called()
