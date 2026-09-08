import uuid
from unittest.mock import AsyncMock, patch

import pytest

from app.core.enums import FileStatus, PermissionLevel
from app.core.exceptions import FileRecordNotFoundError, PermissionDeniedError
from app.services.permission_service import LEVEL_RANK, assert_access


def _fake_file(owner_id: uuid.UUID, status: FileStatus = FileStatus.READY):
    file_obj = AsyncMock()
    file_obj.owner_id = owner_id
    file_obj.status = status
    return file_obj


def _fake_grant(access_level: PermissionLevel):
    grant = AsyncMock()
    grant.access_level = access_level
    return grant


class TestLevelRank:
    def test_total_ordering(self) -> None:
        assert LEVEL_RANK[PermissionLevel.VIEWER] < LEVEL_RANK[PermissionLevel.EDITOR] < LEVEL_RANK[PermissionLevel.OWNER]


class TestAssertAccess:
    async def test_owner_always_succeeds_without_a_grant_lookup(self) -> None:
        owner_id = uuid.uuid4()
        file_obj = _fake_file(owner_id)

        with (
            patch("app.services.permission_service.FileRepository.get", AsyncMock(return_value=file_obj)),
            patch("app.services.permission_service.FilePermissionRepository.get", AsyncMock()) as grant_get,
        ):
            result = await assert_access(AsyncMock(), uuid.uuid4(), owner_id, PermissionLevel.OWNER)

        assert result is file_obj
        grant_get.assert_not_called()

    async def test_viewer_grant_allows_viewer_requirement(self) -> None:
        owner_id, stranger_id = uuid.uuid4(), uuid.uuid4()
        file_obj = _fake_file(owner_id)
        grant = _fake_grant(PermissionLevel.VIEWER)

        with (
            patch("app.services.permission_service.FileRepository.get", AsyncMock(return_value=file_obj)),
            patch("app.services.permission_service.FilePermissionRepository.get", AsyncMock(return_value=grant)),
        ):
            result = await assert_access(AsyncMock(), uuid.uuid4(), stranger_id, PermissionLevel.VIEWER)

        assert result is file_obj

    async def test_viewer_grant_denies_owner_requirement(self) -> None:
        owner_id, stranger_id = uuid.uuid4(), uuid.uuid4()
        file_obj = _fake_file(owner_id)
        grant = _fake_grant(PermissionLevel.VIEWER)

        with (
            patch("app.services.permission_service.FileRepository.get", AsyncMock(return_value=file_obj)),
            patch("app.services.permission_service.FilePermissionRepository.get", AsyncMock(return_value=grant)),
        ):
            with pytest.raises(PermissionDeniedError):
                await assert_access(AsyncMock(), uuid.uuid4(), stranger_id, PermissionLevel.OWNER)

    async def test_stranger_with_no_grant_is_denied(self) -> None:
        owner_id, stranger_id = uuid.uuid4(), uuid.uuid4()
        file_obj = _fake_file(owner_id)

        with (
            patch("app.services.permission_service.FileRepository.get", AsyncMock(return_value=file_obj)),
            patch("app.services.permission_service.FilePermissionRepository.get", AsyncMock(return_value=None)),
        ):
            with pytest.raises(PermissionDeniedError):
                await assert_access(AsyncMock(), uuid.uuid4(), stranger_id, PermissionLevel.VIEWER)

    async def test_missing_file_raises_not_found(self) -> None:
        with patch("app.services.permission_service.FileRepository.get", AsyncMock(return_value=None)):
            with pytest.raises(FileRecordNotFoundError):
                await assert_access(AsyncMock(), uuid.uuid4(), uuid.uuid4(), PermissionLevel.VIEWER)

    async def test_soft_deleted_file_raises_not_found_even_for_owner(self) -> None:
        owner_id = uuid.uuid4()
        file_obj = _fake_file(owner_id, status=FileStatus.DELETED)

        with patch("app.services.permission_service.FileRepository.get", AsyncMock(return_value=file_obj)):
            with pytest.raises(FileRecordNotFoundError):
                await assert_access(AsyncMock(), uuid.uuid4(), owner_id, PermissionLevel.VIEWER)
