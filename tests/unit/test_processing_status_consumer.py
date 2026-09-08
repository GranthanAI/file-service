import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from app.core.enums import FileStatus
from app.core.exceptions import InvalidStateTransitionError
from app.events.processing_status_consumer import process_message
from app.events.topics import FILE_PROCESSING_COMPLETED, FILE_PROCESSING_FAILED, FILE_PROCESSING_STARTED


def _fake_db() -> MagicMock:
    db = MagicMock()
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    return db


def _fake_file(status: FileStatus) -> MagicMock:
    file_obj = MagicMock()
    file_obj.status = status
    file_obj.document_id = uuid.uuid4()
    return file_obj


def _payload(document_id: uuid.UUID, **extra) -> dict:
    return {"event_id": str(uuid.uuid4()), "document_id": str(document_id), **extra}


class TestProcessMessage:
    async def test_started_transitions_uploaded_to_processing(self) -> None:
        db = _fake_db()
        file_obj = _fake_file(FileStatus.UPLOADED)

        with (
            patch("app.events.processing_status_consumer.ProcessedEventRepository.mark_if_new", AsyncMock(return_value=True)),
            patch("app.events.processing_status_consumer.FileRepository.get", AsyncMock(return_value=file_obj)),
            patch("app.events.processing_status_consumer.FileRepository.update_status", AsyncMock()) as update_mock,
        ):
            await process_message(db, FILE_PROCESSING_STARTED, _payload(file_obj.document_id))

        update_mock.assert_awaited_once_with(db, file_obj, FileStatus.PROCESSING)
        db.commit.assert_awaited()

    async def test_completed_transitions_processing_to_ready(self) -> None:
        db = _fake_db()
        file_obj = _fake_file(FileStatus.PROCESSING)

        with (
            patch("app.events.processing_status_consumer.ProcessedEventRepository.mark_if_new", AsyncMock(return_value=True)),
            patch("app.events.processing_status_consumer.FileRepository.get", AsyncMock(return_value=file_obj)),
            patch("app.events.processing_status_consumer.FileRepository.update_status", AsyncMock()) as update_mock,
        ):
            await process_message(db, FILE_PROCESSING_COMPLETED, _payload(file_obj.document_id, chunk_count=42))

        update_mock.assert_awaited_once_with(db, file_obj, FileStatus.READY)

    async def test_failed_transitions_processing_to_failed_with_error_fields(self) -> None:
        db = _fake_db()
        file_obj = _fake_file(FileStatus.PROCESSING)
        payload = _payload(file_obj.document_id, error_code="PARSE_ERROR", error_message="Unsupported PDF encoding")

        with (
            patch("app.events.processing_status_consumer.ProcessedEventRepository.mark_if_new", AsyncMock(return_value=True)),
            patch("app.events.processing_status_consumer.FileRepository.get", AsyncMock(return_value=file_obj)),
            patch("app.events.processing_status_consumer.FileRepository.update_status", AsyncMock()) as update_mock,
        ):
            await process_message(db, FILE_PROCESSING_FAILED, payload)

        update_mock.assert_awaited_once_with(
            db, file_obj, FileStatus.FAILED, last_error_code="PARSE_ERROR", last_error_message="Unsupported PDF encoding"
        )

    async def test_duplicate_event_id_is_a_noop(self) -> None:
        """docs/LLD.md §3.7 idempotency test: a redelivered event_id must not
        double-process — FileRepository.update_status is never even called."""
        db = _fake_db()

        with (
            patch("app.events.processing_status_consumer.ProcessedEventRepository.mark_if_new", AsyncMock(return_value=False)),
            patch("app.events.processing_status_consumer.FileRepository.get", AsyncMock()) as get_mock,
            patch("app.events.processing_status_consumer.FileRepository.update_status", AsyncMock()) as update_mock,
        ):
            await process_message(db, FILE_PROCESSING_COMPLETED, _payload(uuid.uuid4()))

        get_mock.assert_not_called()
        update_mock.assert_not_called()
        db.commit.assert_awaited()

    async def test_unknown_document_id_is_a_noop(self) -> None:
        db = _fake_db()

        with (
            patch("app.events.processing_status_consumer.ProcessedEventRepository.mark_if_new", AsyncMock(return_value=True)),
            patch("app.events.processing_status_consumer.FileRepository.get", AsyncMock(return_value=None)),
            patch("app.events.processing_status_consumer.FileRepository.update_status", AsyncMock()) as update_mock,
        ):
            await process_message(db, FILE_PROCESSING_STARTED, _payload(uuid.uuid4()))

        update_mock.assert_not_called()

    async def test_illegal_transition_is_logged_and_rolled_back_not_raised(self) -> None:
        db = _fake_db()
        file_obj = _fake_file(FileStatus.UPLOADED)  # UPLOADED -> READY is not a valid edge

        with (
            patch("app.events.processing_status_consumer.ProcessedEventRepository.mark_if_new", AsyncMock(return_value=True)),
            patch("app.events.processing_status_consumer.FileRepository.get", AsyncMock(return_value=file_obj)),
            patch(
                "app.events.processing_status_consumer.FileRepository.update_status",
                AsyncMock(side_effect=InvalidStateTransitionError(FileStatus.UPLOADED, FileStatus.READY)),
            ),
        ):
            await process_message(db, FILE_PROCESSING_COMPLETED, _payload(file_obj.document_id))

        db.rollback.assert_awaited_once()
