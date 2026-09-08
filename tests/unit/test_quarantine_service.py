import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from app.core.enums import FileStatus
from app.models.file import File
from app.services.av_scanner import ScanVerdict
from app.services.quarantine_service import (
    AV_SCAN_INFECTED,
    _promote_clean_object,
    _reject_infected_object,
)


def _fake_db() -> MagicMock:
    """A stand-in for AsyncSession supporting exactly what FileRepository/
    OutboxRepository call: sync .add(), async .flush()/.commit()."""
    db = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    return db


def _fake_file(**overrides) -> File:
    defaults = dict(
        document_id=uuid.uuid4(),
        conversation_id=None,
        owner_id=uuid.uuid4(),
        filename="physics.pdf",
        content_type="application/pdf",
        size_bytes=1024,
        storage_bucket="graphgpt-files-quarantine",
        storage_key="owner/doc/v1/original.pdf",
        status=FileStatus.SCANNING,
        etag="etag123",
    )
    defaults.update(overrides)
    return File(**defaults)


class TestPromoteCleanObject:
    async def test_copies_to_files_bucket_and_removes_from_quarantine(self) -> None:
        db = _fake_db()
        minio_client = MagicMock()
        file = _fake_file()

        with patch("app.services.quarantine_service.OutboxRepository.insert", AsyncMock()) as insert_mock:
            await _promote_clean_object(
                db, minio_client, file, bucket="graphgpt-files-quarantine", key=file.storage_key, checksum_sha256="a" * 64
            )

        minio_client.copy_object.assert_called_once()
        minio_client.remove_object.assert_called_once_with("graphgpt-files-quarantine", file.storage_key)
        insert_mock.assert_awaited_once()
        assert insert_mock.call_args.kwargs["event_type"] == "file.uploaded"

    async def test_lands_on_uploaded_never_processing(self) -> None:
        """docs/LLD.md Phase 6 exit criteria: quarantine_service must never
        call transition(SCANNING, PROCESSING) directly — the clean path
        lands on UPLOADED, and only processing_status_consumer.py (Phase 7)
        is allowed to cross into PROCESSING."""
        db = _fake_db()
        minio_client = MagicMock()
        file = _fake_file(status=FileStatus.SCANNING)

        with patch("app.services.quarantine_service.OutboxRepository.insert", AsyncMock()):
            await _promote_clean_object(
                db, minio_client, file, bucket="graphgpt-files-quarantine", key=file.storage_key, checksum_sha256="a" * 64
            )

        assert file.status == FileStatus.UPLOADED
        assert file.status != FileStatus.PROCESSING

    async def test_updates_bucket_and_checksum(self) -> None:
        db = _fake_db()
        minio_client = MagicMock()
        file = _fake_file()

        with patch("app.services.quarantine_service.OutboxRepository.insert", AsyncMock()):
            await _promote_clean_object(
                db, minio_client, file, bucket="graphgpt-files-quarantine", key=file.storage_key, checksum_sha256="b" * 64
            )

        assert file.storage_bucket == "graphgpt-files"
        assert file.checksum_sha256 == "b" * 64


class TestRejectInfectedObject:
    async def test_removes_object_and_lands_on_failed(self) -> None:
        db = _fake_db()
        minio_client = MagicMock()
        file = _fake_file(status=FileStatus.SCANNING)

        with patch("app.services.quarantine_service.OutboxRepository.insert", AsyncMock()) as insert_mock:
            await _reject_infected_object(
                db, minio_client, file, bucket="graphgpt-files-quarantine", key=file.storage_key, signature="Eicar-Signature"
            )

        minio_client.remove_object.assert_called_once_with("graphgpt-files-quarantine", file.storage_key)
        assert file.status == FileStatus.FAILED
        assert file.last_error_code == AV_SCAN_INFECTED
        insert_mock.assert_awaited_once()
        assert insert_mock.call_args.kwargs["event_type"] == "file.scan_failed"

    async def test_never_emits_file_uploaded(self) -> None:
        db = _fake_db()
        minio_client = MagicMock()
        file = _fake_file(status=FileStatus.SCANNING)

        with patch("app.services.quarantine_service.OutboxRepository.insert", AsyncMock()) as insert_mock:
            await _reject_infected_object(
                db, minio_client, file, bucket="graphgpt-files-quarantine", key=file.storage_key, signature="Eicar-Signature"
            )

        published_event_types = {call.kwargs["event_type"] for call in insert_mock.call_args_list}
        assert "file.uploaded" not in published_event_types


class TestScanVerdict:
    def test_clean_and_infected_are_distinguishable(self) -> None:
        assert ScanVerdict(clean=True).signature is None
        assert ScanVerdict(clean=False, signature="Eicar-Signature").signature == "Eicar-Signature"
