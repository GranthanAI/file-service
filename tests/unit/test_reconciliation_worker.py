import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from aiokafka import TopicPartition

from app.core.enums import FileStatus
from app.workers.reconciliation_worker import _check_orphan_backlog


def _record(*, bucket: str, key: str, age_seconds: int) -> MagicMock:
    record = MagicMock()
    record.timestamp = int((datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).timestamp() * 1000)
    record.offset = 42
    record.value = json.dumps(
        {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}
    ).encode()
    return record


class TestCheckOrphanBacklog:
    async def test_unmatched_object_past_grace_period_is_flagged(self) -> None:
        tp = TopicPartition("minio.object.events", 0)
        consumer = MagicMock()
        consumer.getmany = AsyncMock(return_value={tp: [_record(bucket="b", key="k", age_seconds=9999)]})
        consumer.commit = AsyncMock()
        db = AsyncMock()

        with (
            patch("app.workers.reconciliation_worker.settings.RECONCILIATION_ORPHAN_GRACE_SECONDS", 600),
            patch("app.workers.reconciliation_worker.FileRepository.get_by_bucket_and_key", AsyncMock(return_value=None)),
            patch("app.workers.reconciliation_worker.reconciliation_orphans_total") as metric_mock,
        ):
            orphan_count = await _check_orphan_backlog(db, consumer)

        assert orphan_count == 1
        metric_mock.inc.assert_called_once()
        consumer.commit.assert_awaited_once_with({tp: 43})

    async def test_matched_non_uploading_file_is_not_flagged(self) -> None:
        tp = TopicPartition("minio.object.events", 0)
        consumer = MagicMock()
        consumer.getmany = AsyncMock(return_value={tp: [_record(bucket="b", key="k", age_seconds=9999)]})
        consumer.commit = AsyncMock()
        db = AsyncMock()

        matched_file = MagicMock()
        matched_file.status = FileStatus.READY

        with (
            patch("app.workers.reconciliation_worker.settings.RECONCILIATION_ORPHAN_GRACE_SECONDS", 600),
            patch(
                "app.workers.reconciliation_worker.FileRepository.get_by_bucket_and_key",
                AsyncMock(return_value=matched_file),
            ),
            patch("app.workers.reconciliation_worker.reconciliation_orphans_total") as metric_mock,
        ):
            orphan_count = await _check_orphan_backlog(db, consumer)

        assert orphan_count == 0
        metric_mock.inc.assert_not_called()

    async def test_matched_but_still_uploading_file_is_flagged(self) -> None:
        """A row exists but never left UPLOADING — still an orphan per
        docs/LLD.md §4.6 step 3's "status != UPLOADING" check."""
        tp = TopicPartition("minio.object.events", 0)
        consumer = MagicMock()
        consumer.getmany = AsyncMock(return_value={tp: [_record(bucket="b", key="k", age_seconds=9999)]})
        consumer.commit = AsyncMock()
        db = AsyncMock()

        stuck_file = MagicMock()
        stuck_file.status = FileStatus.UPLOADING

        with (
            patch("app.workers.reconciliation_worker.settings.RECONCILIATION_ORPHAN_GRACE_SECONDS", 600),
            patch(
                "app.workers.reconciliation_worker.FileRepository.get_by_bucket_and_key",
                AsyncMock(return_value=stuck_file),
            ),
            patch("app.workers.reconciliation_worker.reconciliation_orphans_total") as metric_mock,
        ):
            orphan_count = await _check_orphan_backlog(db, consumer)

        assert orphan_count == 1
        metric_mock.inc.assert_called_once()

    async def test_event_within_grace_period_is_neither_flagged_nor_committed(self) -> None:
        tp = TopicPartition("minio.object.events", 0)
        consumer = MagicMock()
        consumer.getmany = AsyncMock(return_value={tp: [_record(bucket="b", key="k", age_seconds=5)]})
        consumer.commit = AsyncMock()
        db = AsyncMock()

        with (
            patch("app.workers.reconciliation_worker.settings.RECONCILIATION_ORPHAN_GRACE_SECONDS", 600),
            patch("app.workers.reconciliation_worker.FileRepository.get_by_bucket_and_key", AsyncMock()) as get_mock,
            patch("app.workers.reconciliation_worker.reconciliation_orphans_total") as metric_mock,
        ):
            orphan_count = await _check_orphan_backlog(db, consumer)

        assert orphan_count == 0
        get_mock.assert_not_called()
        metric_mock.inc.assert_not_called()
        consumer.commit.assert_not_awaited()  # left uncommitted for redelivery on a later sweep
