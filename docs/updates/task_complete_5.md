# Task Complete 5: Transactional Outbox + Kafka Producer (Phase 5)

This document provides a detailed log of the reliable event-publishing implementation completed for the **Granthan File Service** under **Phase 5 (Transactional Outbox + Kafka Producer)** of [docs/IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md).

---

## 1. What Was Done

1. **Producer:** [events/producer.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/events/producer.py) — `start_producer()`/`get_producer()`/`stop_producer()`, a process-scoped `AIOKafkaProducer` singleton matching the exact shape of `app/storage/minio_client.py` and `app/db/postgres.py` (module-level instance + getter functions), started/stopped from `lifespan.py`.
2. **Topics:** [events/topics.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/events/topics.py) — constants for the 4 produced event types (`file.uploaded`, `file.deleted`, `file.processing_retry`, `file.scan_failed`) plus `PRODUCED_TOPICS`, and the 3 consumed-but-not-produced processing-status event types (documented for the full contract picture, wired up in Phase 7).
3. **Payload models:** [events/file_events.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/events/file_events.py) — a Pydantic model per produced event type (`FileUploadedEvent`, `FileDeletedEvent`, `FileProcessingRetryEvent`, `FileScanFailedEvent`) plus `EVENT_MODELS`, a registry keyed by `event_type` string.
4. **Publisher:** [events/outbox_publisher.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/events/outbox_publisher.py) — `publish_batch(db, producer, limit=100)`, the poll-and-publish routine from [docs/LLD.md §4.7](../LLD.md#47-outbox-worker-appworkersoutbox_workerpy): fetches unprocessed rows via `outbox_repository.fetch_unprocessed_for_update` (`FOR UPDATE SKIP LOCKED`), stamps `event_id`/`event_type`/`version` onto each row's stored payload via `_build_message`, publishes with `send_and_wait` (waits for the broker ACK), marks each row processed, and commits the whole batch at the end.
5. **Worker:** [workers/outbox_worker.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/workers/outbox_worker.py) — `run_outbox_worker(producer, stop_event)`, a fixed-interval (`OUTBOX_POLL_INTERVAL_MS`) background loop wrapping `publish_batch`, using a fresh `AsyncSession` per poll and swallowing (logging) any exception from a single failed poll rather than killing the loop.
6. **Wiring:** [lifespan.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/lifespan.py) — starts the producer and launches the worker as an `asyncio.Task` at startup; on shutdown, signals the worker's `stop_event`, awaits the task, then stops the producer before disposing the DB engine.
7. **Tests:**
   * [tests/unit/test_outbox_publisher.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_outbox_publisher.py) — `_build_message`'s pure-function behavior: stamps the envelope fields correctly, falls back to raw JSON for an unregistered `event_type`, and raises `pydantic.ValidationError` for a malformed payload against a registered type. No DB/Kafka needed.
   * [tests/integration/test_outbox_publishing.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_outbox_publishing.py) — against a real, ephemeral Kafka (`testcontainers`), matching the three exit criteria exactly: (a) insert a row directly, run `publish_batch`, assert the message appears on the topic and `processed` flips to `TRUE`; (b) a crash-window simulation (send succeeds, then the session is rolled back instead of committed) followed by a second `publish_batch` call, asserting the event is observed **twice** on the topic — documented as expected at-least-once behavior, not a bug; (c) two `publish_batch` calls run concurrently (`asyncio.gather`) against the same 10-row backlog, asserting the rows split between them with no overlap (`SKIP LOCKED` proof).
   * Updated [tests/unit/test_app_bootstrap.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_app_bootstrap.py) and [tests/integration/conftest.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/conftest.py)'s `app_client` fixture to mock `start_producer`/`stop_producer`/`run_outbox_worker` the same way `bootstrap_buckets` was already mocked — see [§2](#2-why-it-was-done) for why this was necessary, not optional polish.

---

## 2. Why It Was Done

* **Mocking the producer/worker in `test_app_bootstrap.py` and the `app_client` fixture was a required fix, not a style choice:** both drive `app.main.app` through its real `lifespan()`, which now calls `await start_producer()` unconditionally at startup. Without the mock, every test using `TestClient(app)` — including all of Phase 3/4's existing upload-flow and permission-matrix tests — would try to open a real connection to `KAFKA_BROKERS` (`localhost:9092` by default) and either hang or fail outright on a machine with no broker listening there. This was caught by actually running the full suite after wiring Phase 5 in, not by inspection — the first attempt correctly reproduced the failure mode before the fixtures were updated.
* **`_build_message` stamps `event_id`/`event_type`/`version` at publish time instead of each service pre-populating them in `outbox_events.payload`:** `upload_service._build_file_uploaded_payload` (Phase 3) already carried a comment anticipating this — "added by the outbox worker (Phase 5) from the outbox_events row itself at publish time, not duplicated here." The row's own `id` and `event_type` columns are already the authoritative values for those two fields; re-deriving them from the row rather than trusting a copy baked into the JSON payload means there's exactly one source of truth and no way for the two to drift apart.
* **`EVENT_MODELS` validation is a deliberate belt-and-suspenders check, with a fallback for the unmapped case:** every event type File Service actually produces today has a model, so in practice every row gets validated before publish — catching a malformed payload (e.g. a future bug in some service's payload builder) before it reaches Kafka rather than after a consumer chokes on it downstream. The `event_type not in EVENT_MODELS` fallback to raw JSON exists so a genuinely new event type introduced in a later phase doesn't hard-fail the whole publisher before its model has been added — but `PRODUCED_TOPICS` membership is still enforced first (an outbox row with a truly unknown `event_type` is logged and skipped, never published), so this fallback only ever applies to a known-produced type that hasn't gotten a model yet, not to arbitrary garbage.
* **A fresh `AsyncSession` per poll in `run_outbox_worker`, not one held open across the loop's lifetime:** matches how every other part of this codebase scopes a session to one unit of work (a single HTTP request via `get_db`, a single test via `db_session`) rather than introducing a new, longer-lived session pattern just for this worker — and it means one bad poll (a transient DB blip, a lock timeout) can't leave a half-broken session wedged in the loop for however long the process keeps running.
* **The crash-window test simulates the failure by rolling back a session that already sent-and-ACKed to real Kafka, then re-running `publish_batch` from scratch** — deliberately not a mocked/fault-injected repository, so the assertion (`the message is observed twice on the topic`) is proven against the actual Kafka broker's actual persisted messages, not against a mock's call log. This is slower to set up than mocking `mark_processed` to raise, but it's the same category of choice already made throughout this codebase's Phase 3/4 integration tests: exercise the real dependency graph wherever the thing being tested is precisely "does the real infrastructure behave as documented."

---

## 3. Code Highlights & Explanations

### 3.1 Stamping the Envelope at Publish Time

Located in: [app/events/outbox_publisher.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/events/outbox_publisher.py)

```python
def _build_message(row: OutboxEvent) -> bytes:
    envelope = {"event_id": str(row.id), "event_type": row.event_type, "version": 1, **row.payload}
    model = EVENT_MODELS.get(row.event_type)
    if model is not None:
        return model.model_validate(envelope).model_dump_json().encode("utf-8")
    return json.dumps(envelope).encode("utf-8")
```
* **Why it matters:** the `**row.payload` spread happens *after* the three stamped fields, so even if a service ever accidentally wrote an `event_id`/`event_type`/`version` key into its own payload dict, the row's own column values still win — there's no ambiguity about which one is authoritative.

### 3.2 The Poll Loop's Shutdown Signal Doubles as Its Sleep

Located in: [app/workers/outbox_worker.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/workers/outbox_worker.py)

```python
try:
    await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
except TimeoutError:
    pass
```
* **Why it matters:** a plain `await asyncio.sleep(poll_interval_seconds)` would make shutdown wait out the full interval even when there's nothing left to do; waiting on the stop event with a timeout gets the same idle-pacing behavior in the common case while letting `lifespan()`'s shutdown path return almost immediately once it calls `stop_event.set()`.

### 3.3 SKIP LOCKED Proven With Real Concurrency, Not a Mock

Located in: [tests/integration/test_outbox_publishing.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_outbox_publishing.py)

```python
async with session_factory() as session_a, session_factory() as session_b:
    published_a, published_b = await asyncio.gather(
        publish_batch(session_a, kafka_producer, limit=100),
        publish_batch(session_b, kafka_producer, limit=100),
    )
assert published_a + published_b == 10
```
* **Why it matters:** two independent connections racing `FOR UPDATE SKIP LOCKED` against the same 10-row backlog is the one scenario where a bug would silently double-publish rows if `SKIP LOCKED` weren't actually working — asserting the totals sum to exactly 10 (not 10-or-more) is a direct proof against the real Postgres locking behavior, not an assumption about it.

---

## 4. Verification

* `uv run pytest`: **91 passed** (60 unit, 31 integration — Phase 5 added 3 unit tests in `test_outbox_publisher.py` and 3 integration tests in `test_outbox_publishing.py`), 0 failures.
* Ran against a real, ephemeral **testcontainers** Kafka broker (`confluentinc/cp-kafka`), alongside the existing ephemeral Postgres + MinIO — confirmed via `docker info` that Docker is available in this environment.
* No regressions: reran the full Phase 0–4 suite after wiring the producer/worker into `lifespan.py` and confirmed the `app_client`/`TestClient` fixtures needed (and got) the same kind of mock `bootstrap_buckets` already had, so none of the earlier phases' tests require a live Kafka broker to pass.
