import json
import uuid

import pytest
from pydantic import ValidationError

from app.events.outbox_publisher import _build_message
from app.models.outbox import OutboxEvent


def _fake_row(event_type: str, payload: dict) -> OutboxEvent:
    row = OutboxEvent(aggregate_id=uuid.uuid4(), event_type=event_type, payload=payload, processed=False)
    row.id = uuid.uuid4()
    return row


class TestBuildMessage:
    def test_stamps_event_id_event_type_version_onto_stored_payload(self) -> None:
        payload = {
            "document_id": str(uuid.uuid4()),
            "conversation_id": None,
            "owner_id": str(uuid.uuid4()),
            "filename": "physics.pdf",
            "content_type": "application/pdf",
            "storage": {"provider": "minio", "bucket": "graphgpt-files", "key": "k", "etag": "e"},
            "timestamp": "2026-09-06T15:00:00+00:00",
        }
        row = _fake_row("file.uploaded", payload)

        message = json.loads(_build_message(row))

        assert message["event_id"] == str(row.id)
        assert message["event_type"] == "file.uploaded"
        assert message["version"] == 1
        assert message["document_id"] == payload["document_id"]

    def test_unregistered_event_type_falls_back_to_raw_json(self) -> None:
        row = _fake_row("file.some_future_event", {"document_id": str(uuid.uuid4())})

        message = json.loads(_build_message(row))

        assert message["event_type"] == "file.some_future_event"
        assert message["document_id"] == row.payload["document_id"]

    def test_malformed_payload_for_a_registered_event_type_raises(self) -> None:
        """docs/LLD.md §1 file_events.py rationale: a malformed row must fail
        loudly here rather than putting bad JSON on the topic."""
        row = _fake_row("file.uploaded", {"document_id": "not-a-uuid"})

        with pytest.raises(ValidationError):
            _build_message(row)
