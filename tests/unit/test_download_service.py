import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from app.core.enums import PermissionLevel
from app.services.download_service import get_download_url


def _fake_file() -> MagicMock:
    file_obj = MagicMock()
    file_obj.storage_bucket = "graphgpt-files"
    file_obj.storage_key = "owner/doc/v1/original.pdf"
    file_obj.filename = "physics.pdf"
    return file_obj


class TestGetDownloadUrl:
    async def test_presigns_against_the_files_bucket_and_key_with_friendly_filename(self) -> None:
        file_obj = _fake_file()
        minio_client = MagicMock()

        with (
            patch("app.services.download_service.permission_service.assert_access", AsyncMock(return_value=file_obj)),
            patch("app.services.download_service.presign_get", return_value="https://minio/signed-url") as presign_mock,
        ):
            url, expires_in = await get_download_url(AsyncMock(), minio_client, uuid.uuid4(), uuid.uuid4())

        assert url == "https://minio/signed-url"
        assert expires_in > 0
        assert presign_mock.call_args.args[1] == "graphgpt-files"
        assert presign_mock.call_args.args[2] == "owner/doc/v1/original.pdf"
        assert presign_mock.call_args.kwargs["response_filename"] == "physics.pdf"

    async def test_requires_at_least_viewer(self) -> None:
        file_obj = _fake_file()

        with (
            patch(
                "app.services.download_service.permission_service.assert_access", AsyncMock(return_value=file_obj)
            ) as assert_access_mock,
            patch("app.services.download_service.presign_get", return_value="url"),
        ):
            await get_download_url(AsyncMock(), MagicMock(), uuid.uuid4(), uuid.uuid4())

        assert assert_access_mock.call_args.args[3] == PermissionLevel.VIEWER
