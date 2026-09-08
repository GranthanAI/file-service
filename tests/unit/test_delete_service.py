import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from minio.error import S3Error

from app.core.enums import FileStatus
from app.core.exceptions import RetentionLockedError
from app.services.delete_service import delete_file


def _fake_file(status: FileStatus = FileStatus.READY) -> MagicMock:
    file_obj = MagicMock()
    file_obj.document_id = uuid.uuid4()
    file_obj.owner_id = uuid.uuid4()
    file_obj.status = status
    return file_obj


def _fake_version(storage_key: str) -> MagicMock:
    version = MagicMock()
    version.storage_key = storage_key
    return version


def _native_version(version_id: str) -> MagicMock:
    native = MagicMock()
    native.version_id = version_id
    return native


def _s3_error(code: str) -> S3Error:
    return S3Error(
        response=None, code=code, message="Object is WORM protected and cannot be overwritten",
        resource="/bucket/key", request_id="req-1", host_id="host-1",
    )


class TestDeleteFile:
    async def test_removes_every_native_version_of_every_key_and_lands_on_deleted(self) -> None:
        file_obj = _fake_file()
        minio_client = MagicMock()
        minio_client.list_objects.return_value = [_native_version("nv-1"), _native_version("nv-2")]
        db = AsyncMock()

        with (
            patch("app.services.delete_service.permission_service.assert_access", AsyncMock(return_value=file_obj)),
            patch("app.services.delete_service.FileRepository.update_status", AsyncMock()) as update_mock,
            patch(
                "app.services.delete_service.FileVersionRepository.list_for_document",
                AsyncMock(return_value=[_fake_version("k/v1/original.pdf")]),
            ),
            patch("app.services.delete_service.OutboxRepository.insert", AsyncMock()) as insert_mock,
        ):
            result = await delete_file(db, minio_client, file_obj.document_id, uuid.uuid4())

        # Two native S3 versions for the one application-level key: both
        # must be individually removed by version_id, not a single
        # version-less call (docs/LLD.md §6.5 — see delete_service's own
        # docstring for why a version-less call isn't sufficient here).
        assert minio_client.remove_object.call_count == 2
        minio_client.remove_object.assert_any_call("graphgpt-files", "k/v1/original.pdf", version_id="nv-1")
        minio_client.remove_object.assert_any_call("graphgpt-files", "k/v1/original.pdf", version_id="nv-2")

        # DELETING first, then DELETED — the terminal call is the DELETED one.
        assert update_mock.call_args_list[0].args[2] == FileStatus.DELETING
        assert update_mock.call_args_list[-1].args[2] == FileStatus.DELETED

        insert_mock.assert_awaited_once()
        assert insert_mock.call_args.kwargs["event_type"] == "file.deleted"
        assert result is file_obj

    @pytest.mark.parametrize("code", ["AccessDenied", "InvalidRequest"])
    async def test_retention_lock_raises_409_and_lands_on_failed(self, code: str) -> None:
        """Both codes are treated as the retention-lock signal — real MinIO
        returns InvalidRequest for a WORM-protected object, not the
        AccessDenied docs/HLD.md §15's prose names (verified directly)."""
        file_obj = _fake_file()
        minio_client = MagicMock()
        minio_client.list_objects.return_value = [_native_version("nv-1")]
        minio_client.remove_object.side_effect = _s3_error(code)
        db = AsyncMock()

        with (
            patch("app.services.delete_service.permission_service.assert_access", AsyncMock(return_value=file_obj)),
            patch("app.services.delete_service.FileRepository.update_status", AsyncMock()) as update_mock,
            patch(
                "app.services.delete_service.FileVersionRepository.list_for_document",
                AsyncMock(return_value=[_fake_version("k/v1/original.pdf")]),
            ),
            patch("app.services.delete_service.OutboxRepository.insert", AsyncMock()) as insert_mock,
        ):
            with pytest.raises(RetentionLockedError):
                await delete_file(db, minio_client, file_obj.document_id, uuid.uuid4())

        assert update_mock.call_args_list[-1].args[2] == FileStatus.FAILED
        insert_mock.assert_not_called()  # no file.deleted event on failure

    async def test_non_retention_s3_error_propagates_unmodified(self) -> None:
        file_obj = _fake_file()
        minio_client = MagicMock()
        minio_client.list_objects.return_value = [_native_version("nv-1")]
        minio_client.remove_object.side_effect = _s3_error("InternalError")
        db = AsyncMock()

        with (
            patch("app.services.delete_service.permission_service.assert_access", AsyncMock(return_value=file_obj)),
            patch("app.services.delete_service.FileRepository.update_status", AsyncMock()),
            patch(
                "app.services.delete_service.FileVersionRepository.list_for_document",
                AsyncMock(return_value=[_fake_version("k/v1/original.pdf")]),
            ),
        ):
            with pytest.raises(S3Error) as exc_info:
                await delete_file(db, minio_client, file_obj.document_id, uuid.uuid4())

        assert exc_info.value.code == "InternalError"
