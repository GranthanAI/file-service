# Task Complete 7: Processing Status Contract — Consumer Side (Phase 7)

This document logs the Retrieval Service event-contract implementation completed for the **Granthan File Service** under **Phase 7 (Processing Status Contract, Consumer Side)** of [docs/IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md).

---

## 1. What Was Done

1. **Dedupe table + migration:** [models/processed_event.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/models/processed_event.py) — `ProcessedEventId`, a composite `(event_id, consumer)` primary key rather than `event_id` alone, so a future second consumer of the same topic (docs/LLD.md §3.7: "a hard requirement for any future consumer") tracks its own dedupe state independently. [db/migrations/versions/17fe3249e29f_create_processed_event_ids_table.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/db/migrations/versions/17fe3249e29f_create_processed_event_ids_table.py). [repositories/processed_event_repository.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/repositories/processed_event_repository.py) — `mark_if_new`, a single `INSERT ... ON CONFLICT DO NOTHING RETURNING` statement (atomic check-and-record, no separate SELECT that could race).
2. **The consumer:** [events/processing_status_consumer.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/events/processing_status_consumer.py) — `process_message` (dedupe → look up the file → dispatch by **topic**, not by a payload field, since docs/LLD.md §3.6's three payload shapes carry no `event_type` key) and `run_processing_status_consumer`, a background task subscribed to all three `file.processing.*` topics at once.
3. **Wiring:** [lifespan.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/lifespan.py) — started unconditionally (unlike Phase 6's quarantine consumer), since Retrieval Service processing happens regardless of `AV_SCAN_ENABLED`.
4. **Retry:** [services/retry_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/retry_service.py) — `retry_upload` (docs/LLD.md §4.4): permission check, `FAILED -> PROCESSING`, publish `file.processing_retry`, increment `retry_count`; never sets `READY` itself. [api/routers/retry.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/routers/retry.py) — `POST /files/{document_id}/retry`, reusing `FileStatusResponse` rather than a new schema (identical wire shape).
5. **New exception:** `RetryNotAllowedError` (docs/LLD.md §5.7's `409` when current status isn't `FAILED`) — see [§2](#2-why-it-was-done) for why this isn't just `InvalidStateTransitionError`.
6. **Tests:**
   * [tests/unit/test_processing_status_consumer.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_processing_status_consumer.py) — all three topic branches, duplicate-event no-op, unknown-document no-op, and an illegal-transition path logged + rolled back rather than raised out of the handler.
   * [tests/unit/test_retry_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_retry_service.py) — the FAILED-only precondition, correct `retry_count` arithmetic in the published payload, and that a rejected retry never touches the outbox/retry-count.
   * [tests/unit/test_processing_ownership_regression.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_processing_ownership_regression.py) — the Phase 7 exit criterion's regression guard, done as a source-text check (see [§2](#2-why-it-was-done)) rather than a behavioral one.
   * [tests/integration/test_processing_status_flow.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_processing_status_flow.py) — a real Kafka broker, a real running `run_processing_status_consumer` task, and a "fake Retrieval Service" that's just this test file publishing `file.processing.started`/`.completed`/`.failed` directly to the real topics: drives `UPLOADED -> PROCESSING -> READY`, proves the failure path sets both error fields, and redelivers the same `event_id` twice to prove the second delivery is a no-op.
   * [tests/integration/test_retry_api.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_retry_api.py) — the full HTTP round trip: owner retry succeeds (202, `outbox_events` has the `file.processing_retry` row with the right `retry_count`), non-`FAILED` retry is `409`, a stranger is `403`, an `EDITOR` grant is sufficient, a `VIEWER` grant is not.

---

## 2. Why It Was Done

* **`RetryNotAllowedError` instead of reusing `InvalidStateTransitionError`:** the underlying state machine (`lifecycle_service.ALLOWED_TRANSITIONS`) actually *permits* `READY -> PROCESSING` (documented as the legitimate re-index/re-processing edge, docs/LLD.md §4.2) — so a plain `transition()` call would not reject a retry attempted from `READY`, even though docs/LLD.md §4.4/§5.7 are explicit that retry is FAILED-only. This is a business precondition specific to the `/retry` endpoint, not a state-machine edge violation, so it's enforced by an explicit check in `retry_service.py` and reported with its own exception — reusing `InvalidStateTransitionError` here would have produced a technically-true but conceptually misleading message ("cannot transition from READY to PROCESSING") for an edge the table itself allows via a different caller.
* **Dispatch by Kafka topic, not by a payload field:** checked docs/LLD.md §3.6's three example payloads directly rather than assuming a conventional `event_type` field would be present — none of the three shown JSON bodies carry one (unlike `file.uploaded`'s own payload, which does, per docs/HLD.md §13). Building the dispatcher around a field that doesn't exist would have been a bug caught only at integration-test time; reading the topic from `record.topic` (via `AIOKafkaConsumer.getmany()`'s per-`TopicPartition` grouping) is what the actual message shape supports.
* **The regression test is a source-text check, not a behavioral one:** the requirement itself ("`quarantine_service` never calls `transition(SCANNING, PROCESSING)` directly", extended here to `upload_service`/`retry_service`+`READY`) is about what these modules are *capable of doing*, not just what their current happy path exercises. A behavioral test only proves today's code path doesn't hit the edge; a static check on the literal string `"FileStatus.PROCESSING"`/`"FileStatus.READY"` inside those specific modules' source (via `inspect.getsource`) directly encodes "this module must never even attempt the target," which is what the LLD's design-review-driven rule is actually protecting against reintroducing.
* **`retry_service` requires `EDITOR`, not `OWNER`:** docs/LLD.md §5.7 doesn't list a permission error at all for `/retry` (only the `409` state-precondition), but §9's blanket policy ("every read/write route resolves through `permission_service.assert_access`") still applies to every route, documented or not. Retry mutates processing state without granting/revoking access or deleting anything — closer in blast radius to "editing" than to the owner-only actions (`PUT /permissions`, `DELETE`) — so `EDITOR` was the more consistent choice than defaulting to `OWNER` out of caution; documented in `retry_service.py` itself so the reasoning travels with the code.
* **`run_processing_status_consumer` is started unconditionally in `lifespan.py`, unlike Phase 6's quarantine consumer:** Retrieval Service's processing events are produced regardless of whether AV scanning is enabled — `file.uploaded` is always eventually emitted (either directly from `/complete` or via quarantine promotion), so there's always a reason for this consumer to be listening. This is also why it — like the outbox worker, but *unlike* the quarantine consumer — needed to be mocked in `test_app_bootstrap.py` and the `app_client` integration fixture: it isn't gated behind a feature flag the way the quarantine consumer is, so every test that boots the real app would otherwise need a live Kafka broker.

---

## 3. A Real Bug Found by Testing

### 3.1 The Kafka Consumer's `bootstrap_servers` Wasn't Reachable From Tests

**Symptom:** the very first `test_processing_status_flow.py` run failed with `KafkaConnectionError: Unable to bootstrap from [('localhost', 9092, ...)]` — `run_processing_status_consumer` builds its own `AIOKafkaConsumer` internally, reading `settings.KAFKA_BROKERS` at call time, but the test's ephemeral `testcontainers` Kafka broker listens on a random host port, not `9092`.

**Why it wasn't an issue for the outbox worker's tests:** `outbox_publisher.publish_batch` takes an already-constructed `AIOKafkaProducer` as a parameter — the test simply constructs one pointed at the test broker and passes it in. The consumer, by contrast, owns its own client construction, so there was no injection point.

**Fix:** the test patches `settings.KAFKA_BROKERS` (via `unittest.mock.patch.object`, since `Settings` is a mutable Pydantic model) for the duration of the consumer task, using an `asynccontextmanager` helper (`_running_consumer`) that starts the task under the patched setting and guarantees `stop_event.set()` + task cleanup even on failure. This is the same category of fixture-level workaround `app_client`'s `dependency_overrides` already use for the DB/MinIO — settings-driven internal construction just doesn't have FastAPI's DI to hook into, so the test reaches for `unittest.mock.patch.object` instead.

### 3.2 The Test's Own Seed Data Was Wrong, Not the Consumer

**Symptom:** after fixing the Kafka connectivity issue, every test still failed — with the consumer logging `processing_status_consumer.illegal_transition current=UPLOADING target=PROCESSING` for every message.

**Root cause:** the test's `_insert_uploaded_file` helper only called `FileRepository.insert` (which defaults to `UPLOADING`, per docs/LLD.md §2.1's `server_default`) and never actually transitioned the row to `UPLOADED` — so the consumer's `transition(UPLOADING, PROCESSING)` call correctly rejected an edge that genuinely isn't in `ALLOWED_TRANSITIONS`. This was the test's own setup bug, not a defect in `processing_status_consumer.py` — but it's worth recording because it's exactly the kind of gap unit tests (which mock `FileRepository.get` to return whatever status the test wants directly) can't catch: only a real end-to-end flow through the actual repository surfaces a seed-data mistake like this.

**Fix:** the helper now calls `FileRepository.update_status(db, file_obj, FileStatus.UPLOADED, ...)` after insert, matching what a real file looks like once it's actually reached the state `file.processing.started` is documented to fire from.

---

## 4. Verification

* `uv run pytest`: **129 passed** (87 unit, 42 integration — Phases 0–7 combined), 0 failures. Phase 7 alone added 3 new unit test files (`test_processing_status_consumer.py`, `test_retry_service.py`, `test_processing_ownership_regression.py`) and 2 new integration test files (`test_processing_status_flow.py`, `test_retry_api.py`).
* Ran against real, ephemeral **testcontainers** Postgres + Kafka — the processing-status flow tests exercise the actual background consumer task (`run_processing_status_consumer`), not a mocked dispatcher.
* `alembic upgrade head` (via the `migrated_database_url` fixture) confirmed clean on the new `processed_event_ids` migration, chained after Phase 1's original migration.
* No regressions: reran the full Phase 0–6 suite after wiring Phase 7 in.
