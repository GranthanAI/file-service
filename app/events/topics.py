"""docs/LLD.md §1: topic name constants for the Kafka event contract.

Topic name == event_type value today — there is no separate topic-naming
scheme layered on top of `outbox_events.event_type` — but `PRODUCED_TOPICS`
is still the single place that fact is asserted, so outbox_publisher.py can
fail loudly on an outbox row whose event_type isn't one File Service
actually produces, rather than silently publishing to a bogus topic string.
"""

# Produced by File Service (outbox-driven, docs/LLD.md §4.7/§13 in HLD).
FILE_UPLOADED = "file.uploaded"
FILE_DELETED = "file.deleted"
FILE_PROCESSING_RETRY = "file.processing_retry"
FILE_SCAN_FAILED = "file.scan_failed"

PRODUCED_TOPICS: frozenset[str] = frozenset(
    {FILE_UPLOADED, FILE_DELETED, FILE_PROCESSING_RETRY, FILE_SCAN_FAILED}
)

# Consumed by File Service — published by Retrieval Service (docs/LLD.md
# §3.6). Not produced here; listed for the full picture of the event
# contract. Wired up by processing_status_consumer.py in Phase 7.
FILE_PROCESSING_STARTED = "file.processing.started"
FILE_PROCESSING_COMPLETED = "file.processing.completed"
FILE_PROCESSING_FAILED = "file.processing.failed"
