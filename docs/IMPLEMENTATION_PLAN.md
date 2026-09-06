# File Service — Implementation Plan (Coding Phases)

This document breaks [`LLD.md`](./LLD.md) into an ordered sequence of coding phases for the **Granthan File Service**. Each phase is scoped so it can be built, tested, and merged independently — later phases depend only on earlier ones, never the reverse. File/module paths reference the directory tree in [`LLD.md §1`](./LLD.md#1-directory-to-module-mapping-where-why-and-how).

Conventions carried over from `auth-service`/`memory-service`: **FastAPI**, **SQLAlchemy (async) + PostgreSQL**, **Alembic**, **aiokafka**, **uv** for dependency management, `pyproject.toml`, `Dockerfile` + `docker-compose.yml` for local infra.

---

## Phase Overview

| Phase | Name | Depends on | Can be tested in isolation? |
|---|---|---|---|
| 0 | Project bootstrap & local infra | — | Service boots, `/health/live` returns 200 |
| 1 | Database layer (models, migrations, repositories) | 0 | Repository unit tests against a test DB |
| 2 | MinIO storage layer | 0 | Unit tests with mocked/real MinIO |
| 3 | Upload flow (initiate + complete, no AV scan) | 1, 2 | End-to-end upload works, `AV_SCAN_ENABLED=false` |
| 4 | Lifecycle, permissions, metadata APIs | 1, 3 | Get/list/status/permission endpoints work |
| 5 | Transactional outbox + Kafka producer | 1 | `file.uploaded` observed on a test topic |
| 6 | Quarantine / AV scan flow | 2, 3, 5 | Clean & infected files behave per §4.5 |
| 7 | Processing status contract (consumer side) | 4, 5 | `PROCESSING`/`READY`/`FAILED` driven by fake events |
| 8 | Download, delete, retry, versioning | 2, 4, 5 | Full API surface complete |
| 9 | Reconciliation worker | 3, 5, 6 | Orphans/expired jobs get swept |
| 10 | Observability (logs, metrics, tracing, health) | all above | Dashboards/alerts wired |
| 11 | Security hardening pass | all above | Checklist in LLD §9 fully satisfied |
| 12 | Testing (unit/integration/contract) | all above | CI green, coverage target met |
| 13 | Deployment & CI/CD | all above | Deploys to staging via pipeline |

Each phase below lists: **Goal**, **Tasks**, **Files touched**, **Exit criteria (Definition of Done)**, and **LLD references**.

---

## Phase 0 — Project Bootstrap & Local Infrastructure

**Goal:** A running, empty FastAPI service with all infra dependencies available locally, matching the shape of `auth-service`/`memory-service`.

**Tasks:**
1. Scaffold the directory tree from LLD §1 (empty `__init__.py` files, no logic yet).
2. `pyproject.toml` with dependencies: `fastapi`, `uvicorn`, `pydantic[email]`, `pydantic-settings`, `sqlalchemy[asyncio]`, `asyncpg`, `alembic`, `minio`, `aiokafka`, `python-magic` (or `filetype`) for magic-byte sniffing.
3. `app/core/config.py` — Pydantic Settings covering the full table in LLD §7 (DB URL, MinIO endpoint/keys/buckets, Kafka brokers, upload limits, TTLs, feature flags).
4. `app/core/logging.py` — structured JSON logging (request_id/document_id/owner_id fields, per LLD §21 in HLD).
5. `app/main.py` + `app/lifespan.py` — app factory, empty router registration, DB engine + MinIO client construction stubs (workers wired in later phases).
6. `docker-compose.yml` — Postgres, MinIO (single-node, matching HLD §5.2), Kafka (reuse from `conversation-service` if already running locally, else standalone), and the file-service app container.
7. `docker/minio/init-buckets.sh` — creates `graphgpt-files` / `graphgpt-files-quarantine`, applies the deny-public policy, for local bring-up (mirrors `bucket_bootstrap.py`'s desired state so local dev matches prod bootstrap behavior).
8. `Dockerfile`, `Makefile`, `.env.example`, `README.md`.
9. Alembic initialized (`alembic.ini`, `app/db/migrations/env.py`) — no migrations yet.
10. `GET /health/live` implemented (no dependencies) — this is the only route that exists at the end of this phase.

**Exit criteria:**
- `docker compose up` brings up Postgres + MinIO + Kafka + app cleanly.
- `curl localhost:PORT/health/live` returns `200`.
- `uv run pytest` runs (even with zero tests) without import errors.

**LLD refs:** §1 (directory tree), §7 (config table).

---

## Phase 1 — Database Layer

**Goal:** All PostgreSQL tables exist and are reachable through repositories, independent of any HTTP or MinIO logic.

**Tasks:**
1. `app/models/file.py`, `upload_job.py`, `file_version.py`, `file_permission.py`, `outbox.py` — SQLAlchemy models exactly matching LLD §2.1–§2.5, including `retry_count`, `last_error_code`, `last_error_message`, `etag`, `checksum_sha256`.
2. `app/core/enums.py` — `FileStatus` (including `SCANNING`), `PermissionLevel`.
3. `app/db/base.py` — Declarative base importing all models (for Alembic autogenerate).
4. Alembic migration: create `file_status` enum, `permission_level` enum, all 5 tables, all indexes from LLD §2 (including the partial index on `files.status` and the unique index on `(storage_bucket, storage_key)`).
5. `app/repositories/*.py` — one repository per table, exposing only the query methods each service actually needs (no generic CRUD base class — keep it explicit, matching `auth-service`'s repository style):
   - `file_repository`: `insert`, `get`, `get_for_update`, `update_status` (delegates to `lifecycle_service.transition` before writing), `increment_retry_count`, `list_paginated` (keyset on `document_id`).
   - `upload_job_repository`: `insert`, `get_pending`, `mark_completed`, `expire_stale`.
   - `file_version_repository`: `insert`, `list_for_document`.
   - `file_permission_repository`: `get`, `upsert`, `revoke`.
   - `outbox_repository`: `insert`, `fetch_unprocessed_for_update` (the `SKIP LOCKED` query from LLD §4.7), `mark_processed`.
6. `app/utils/id_generator.py` — UUIDv7 generation (`new_uuid7()`), unit-tested for monotonicity.

**Exit criteria:**
- `alembic upgrade head` runs clean against a fresh Postgres.
- Unit tests: insert a `files` row, read it back, keyset-paginate 3 rows correctly, verify the partial index is used by `EXPLAIN` for a status filter (sanity check, not a hard gate).
- `id_generator` unit test confirms UUIDv7 ordering matches insertion order.

**LLD refs:** §2 (all subsections), §8 (document ID).

---

## Phase 2 — MinIO Storage Layer

**Goal:** All object-storage operations (`storage/`) work in isolation, fully unit-testable with a real or mocked MinIO, with zero dependency on the DB or HTTP layer.

**Tasks:**
1. `app/storage/minio_client.py` — process-scoped client construction (LLD §3.1).
2. `app/storage/bucket_bootstrap.py` — idempotent bucket creation, versioning, SSE, lifecycle rule on quarantine, deny-public policy, notification config (LLD §3.2). Wire into `lifespan.py` startup.
3. `app/storage/key_builder.py` — pure function `build_storage_key(owner_id, document_id, version, filename)` (LLD §3.3). Unit tests only — no I/O.
4. `app/storage/presign.py` — `presign_put`, `presign_post_policy`, `presign_get` wrappers (LLD §3.4).
5. `app/storage/object_verifier.py` — `verify_uploaded_object` with **exact normalized content-type comparison** (LLD §3.5) and the `_normalize_content_type` helper.
6. `app/validation/content_type.py`, `size_limits.py` — allowlist + size-cap checks, independent of `storage/` (LLD's separation rationale in §1).
7. `app/validation/magic_bytes.py` — sniffing function only; **not wired into `/complete` yet** (that would violate the trust-chain ordering in LLD §3.5) — wired in Phase 6 where the AV-scan pass actually reads bytes.

**Exit criteria:**
- Integration test (testcontainers MinIO): presign a PUT, upload a small file via `requests`/`httpx`, `stat_object` confirms size/etag.
- Unit test proves `_normalize_content_type` rejects `application/pdf-malicious` against expected `application/pdf` (regression test for the exact-match fix).
- `bucket_bootstrap` is idempotent: running it twice against the same MinIO produces no errors and no duplicate config.

**LLD refs:** §3 (all subsections).

---

## Phase 3 — Upload Flow (Initiate + Complete, AV Scan Disabled)

**Goal:** The full upload round trip works end-to-end with `AV_SCAN_ENABLED=false` — this is deliberately the simpler path first, so the core transaction/verification logic is proven before quarantine complexity is layered on in Phase 6.

**Tasks:**
1. `app/api/schemas/upload.py` — `InitiateUploadRequest/Response`, `CompleteUploadRequest`.
2. `app/services/upload_service.py` — `initiate_upload` and `complete_upload` exactly as specified in LLD §4.1, with the `AV_SCAN_ENABLED` branch present but only the `false` branch exercised in this phase.
3. `app/core/exceptions.py` — the exception set from LLD §8 that's relevant here: `InvalidContentTypeError`, `UploadSizeExceededError`, `AlreadyCompletedError`, `PresignExpiredError`, `ObjectNotFoundError`, `SizeMismatchError`, `ContentTypeMismatchError`, plus the central FastAPI exception-handler registration.
4. `app/api/routers/upload.py` — `POST /files/upload/initiate`, `POST /files/{document_id}/complete`.
5. `app/api/deps.py` — `get_db`, `get_current_user` (JWT dependency, verifying against Auth Service's public key per the pattern in `auth-service`).

**Exit criteria:**
- Integration test: initiate → PUT bytes directly to MinIO → complete → assert `files.status == UPLOADED`, `file_versions` has 1 row, `outbox_events` has exactly 1 `file.uploaded` row.
- Idempotency test: calling `/complete` twice returns the same `200` response both times, no duplicate `file_versions`/`outbox_events` rows.
- Failure test: complete without ever uploading bytes → `422 ObjectNotFoundError`; upload wrong size → `422 SizeMismatchError`.

**LLD refs:** §4.1, §5.1–§5.2, §6.1.

---

## Phase 4 — Lifecycle, Permissions, Metadata APIs

**Goal:** Read-side and authorization are complete: a client can look up, list, and reason about files it owns or has been granted access to.

**Tasks:**
1. `app/services/lifecycle_service.py` — `ALLOWED_TRANSITIONS` table and `transition()` exactly as in LLD §4.2, including the `SCANNING → UPLOADED` (not `→ PROCESSING`) correction and the idempotent `current == target` no-op.
2. `app/services/permission_service.py` — `assert_access` (USER-only, per LLD §2.4/§4.3 scope decision).
3. `app/api/deps.py` — add `require_permission(level)` dependency factory.
4. `app/services/file_service.py` — `get_file`, `list_files` (keyset pagination).
5. `app/api/schemas/file.py`, `permission.py`, `common.py`.
6. `app/api/routers/files.py` — `GET /files/{document_id}`, `GET /files`.
7. `app/api/routers/status.py` — `GET /files/{document_id}/status`.
8. `app/api/routers/permissions.py` — `PUT /files/{document_id}/permissions`.

**Exit criteria:**
- Permission test matrix: owner access always succeeds; a `VIEWER` grant allows `GET` but not `PUT permissions`/`DELETE`; a stranger gets `403` on everything.
- Lifecycle test: every edge in `ALLOWED_TRANSITIONS` is exercised once (parametrized test), and every non-edge raises `InvalidStateTransitionError`.
- List pagination test: 25 files, `limit=10`, 3 pages via `next_cursor`, no duplicates/gaps.

**LLD refs:** §2.4, §4.2, §4.3, §5.3, §5.4, §5.6, §5.9.

---

## Phase 5 — Transactional Outbox + Kafka Producer

**Goal:** Outbox rows written in earlier phases actually reach Kafka, reliably, with the documented at-least-once semantics.

**Tasks:**
1. `app/events/producer.py` — `aiokafka` producer wrapper, started/stopped in `lifespan.py`.
2. `app/events/topics.py` — topic constants for all produced events: `file.uploaded`, `file.deleted`, `file.processing_retry`, `file.scan_failed`.
3. `app/events/file_events.py` — Pydantic payload models per event type (used both to build outbox payloads and in contract tests, Phase 12).
4. `app/events/outbox_publisher.py` — the poll-and-publish routine using `outbox_repository.fetch_unprocessed_for_update` (`FOR UPDATE SKIP LOCKED`, LLD §4.7).
5. `app/workers/outbox_worker.py` — background loop wrapping the publisher, started from `lifespan.py`.

**Exit criteria:**
- Integration test (testcontainers Kafka): insert an `outbox_events` row directly, start the worker, assert the message appears on the topic and `processed` flips to `TRUE`.
- Crash-window test: kill the worker between "Kafka ACK" and "mark processed" (simulate via a fault-injected repository) — assert the event is republished on the next poll, and document this in the test as **expected** behavior (at-least-once, LLD §3.7), not a bug.
- Multi-worker test: run two worker instances concurrently against the same outbox backlog — assert no row is published twice (`SKIP LOCKED` proof).

**LLD refs:** §3.7, §4.7, §13 (from HLD).

---

## Phase 6 — Quarantine / AV Scan Flow

**Goal:** The single authoritative upload flow from LLD §4.5 is fully implemented: uploads default to the quarantine bucket, get scanned, and only clean files are promoted and trigger `file.uploaded`.

**Tasks:**
1. Flip `upload_service.initiate_upload`'s target bucket to `BUCKET_QUARANTINE` whenever `AV_SCAN_ENABLED=true` (this was stubbed in Phase 3).
2. `app/events/topics.py` — add the MinIO bucket-notification consumer topic (`minio.object.events`, or whatever `MINIO_KAFKA_NOTIFICATION_ARN` resolves to).
3. `app/services/quarantine_service.py` — full implementation of the flow diagrammed in LLD §4.5/§6.6:
   - Consume `ObjectCreated` for the quarantine bucket.
   - `lifecycle_service.transition(UPLOADED → SCANNING)`.
   - Stream the object once: run `magic_bytes` sniffing (now wired in, per Phase 2's deferred step) **and** the AV scan (ClamAV client or equivalent) **and** compute `checksum_sha256` — one read, three outputs.
   - Clean: `CopyObject` to `graphgpt-files`, update `storage_bucket`/`checksum_sha256`, `RemoveObject` from quarantine, `lifecycle_service.transition(SCANNING → UPLOADED)`, insert `outbox_events(file.uploaded)`.
   - Infected: `RemoveObject`, `lifecycle_service.transition(SCANNING → FAILED)` with `last_error_code='AV_SCAN_INFECTED'`, insert `outbox_events(file.scan_failed)`.
4. Local dev AV backend: a ClamAV container in `docker-compose.yml`, or a pluggable scanner interface with a no-op "always clean" implementation for environments without ClamAV.

**Exit criteria:**
- Integration test: upload an EICAR test file → assert it ends in `FAILED` with `last_error_code='AV_SCAN_INFECTED'`, `file.uploaded` is never on the topic for that `document_id`, only `file.scan_failed` is.
- Integration test: upload a real clean PDF → assert `graphgpt-files` has the object, quarantine bucket does not, `checksum_sha256` is populated, `status == UPLOADED`, and exactly one `file.uploaded` event is on the topic.
- Regression test explicitly asserting `quarantine_service` never calls `transition(SCANNING, PROCESSING)` directly (guards against the state-machine regression caught in the design review).

**LLD refs:** §3.5, §3.6 (boundary), §4.5, §6.6.

---

## Phase 7 — Processing Status Contract (Consumer Side)

**Goal:** File Service correctly reacts to Retrieval Service's processing events, closing the loop described in LLD §3.6.

**Tasks:**
1. `app/events/processing_status_consumer.py` — consumes `file.processing.started` / `file.processing.completed` / `file.processing.failed`, each handler:
   - Deduplicates on `event_id` (small `processed_event_ids` table or upsert-based dedupe, per LLD §3.7).
   - `file.processing.started` → `transition(UPLOADED → PROCESSING)`.
   - `file.processing.completed` → `transition(PROCESSING → READY)`.
   - `file.processing.failed` → `transition(PROCESSING → FAILED)`, set `last_error_code`/`last_error_message` from payload.
2. Migration: add `processed_event_ids` table (or equivalent dedupe mechanism) if not folded into existing tables.
3. Wire the consumer as a background task in `lifespan.py`, alongside the outbox worker.
4. `app/services/retry_service.py` — `POST /files/{document_id}/retry` implementation (LLD §4.4): `FAILED → PROCESSING`, publish `file.processing_retry`, increment `retry_count`. Explicitly does **not** touch `READY`.
5. `app/api/routers/retry.py`.

**Exit criteria:**
- Integration test using a fake Retrieval Service (a test script publishing the three event types) drives a file through `UPLOADED → PROCESSING → READY`.
- Failure-path test: `file.processing.failed` moves a file to `FAILED` with the error fields populated; `POST /retry` moves it back to `PROCESSING`; a subsequent `file.processing.completed` moves it to `READY`.
- Idempotency test: publish `file.processing.completed` twice for the same `event_id` — second delivery is a no-op (no double-processing, no error).
- Regression test: assert no code path outside `processing_status_consumer.py` ever calls `transition(UPLOADED, PROCESSING)` or `transition(PROCESSING, READY)`.

**LLD refs:** §3.6, §3.7, §4.4, §6.7.

---

## Phase 8 — Download, Delete, Retry Wiring, Versioning

**Goal:** Remaining API surface from LLD §5 is complete.

**Tasks:**
1. `app/services/download_service.py` — `get_download_url` (permission check + `presign_get`, LLD §4.1/§6.4 pattern).
2. `app/api/routers/download.py` — `GET /files/{document_id}/download`.
3. `app/services/version_service.py` — new-version upload path (`initiate_upload` accepting an existing `document_id` to create `v{n+1}`, per LLD §16 in HLD/§2.3 in LLD).
4. Delete flow: extend `file_service.py` (or a new `app/services/delete_service.py`) implementing LLD §6.5 — `DELETING`, `RemoveObject` (all versions), retention-lock handling (`409` on `AccessDenied`), `DELETED`, `outbox: file.deleted`.
5. `app/api/routers/files.py` — add `DELETE /files/{document_id}`.

**Exit criteria:**
- Integration test: full lifecycle `upload → download (VIEWER can, stranger 403) → new version → list shows both versions → delete → subsequent GET returns 404`.
- Retention-lock test: object-locked file returns `409` on delete and stays in `FAILED` (not silently stuck in `DELETING`).

**LLD refs:** §4.5 in HLD (versioning), §5.5, §5.8, §6.5.

---

## Phase 9 — Reconciliation Worker

**Goal:** Operational safety net for abandoned uploads and orphaned objects, without ever doing a full-bucket `LIST`.

**Tasks:**
1. `app/workers/reconciliation_worker.py` — implements LLD §4.6 exactly:
   - Expire stale `upload_jobs`.
   - Fail abandoned `UPLOADING` files past a timeout.
   - Consume the MinIO notification backlog and cross-check against `files`, emitting `reconciliation_orphans_total` for anything unmatched past a grace period.
2. Wire into `lifespan.py` as a periodic background task (`RECONCILIATION_INTERVAL_SECONDS`).

**Exit criteria:**
- Test: create an `upload_jobs` row with `expires_at` in the past, run one sweep, assert it flips to `EXPIRED`.
- Test: create a `files` row in `UPLOADING` with `created_at` > 1h ago and no job, run one sweep, assert it flips to `FAILED`.
- Test/assert (static check, not runtime): no call to `list_objects`/`ListObjectsV2` exists anywhere in `reconciliation_worker.py` — codified as a lint rule or a simple grep-based test if useful.

**LLD refs:** §4.6.

---

## Phase 10 — Observability

**Goal:** Metrics, logs, tracing, and health checks match LLD §4.8 and HLD §21.

**Tasks:**
1. `app/api/routers/health.py` — `GET /health/live`, `GET /health/ready` (DB + MinIO only, Kafka excluded — LLD §4.8).
2. Metrics: `upload_requests_total`, `upload_failures_total{reason}`, `upload_duration_seconds`, `minio_stat_object_duration_seconds`, `minio_presign_duration_seconds`, `file_size_bytes`, `processing_status_count{status}`, `download_requests_total`, `delete_requests_total`, `outbox_publish_lag_seconds`, `reconciliation_orphans_total` — exposed via a Prometheus client on a `/metrics` endpoint.
3. Structured logging: ensure every log line in every service/worker carries `request_id`/`document_id`/`owner_id`/`event_id` where applicable.
4. Tracing: instrument the BFF → File Service → PostgreSQL/MinIO → Kafka path (OpenTelemetry, matching whatever tracing backend the rest of GraphGPT uses).

**Exit criteria:**
- `/metrics` scrapeable and shows non-zero counters after running the Phase 3/6/7 integration tests.
- `/health/ready` returns `503` when Postgres or MinIO is down, and `200` when only Kafka is down (explicit test for the readiness fix from the design review).

**LLD refs:** §4.8; HLD §21.

---

## Phase 11 — Security Hardening Pass

**Goal:** Explicit checklist pass against LLD §9 before calling the service production-ready.

**Tasks — verify each, don't just assume from earlier phases:**
1. MinIO credentials used by the service are the scoped IAM policy from HLD §14, not root.
2. `bucket_bootstrap` denies public/anonymous access — verify with an unauthenticated `curl` against a known key.
3. Presign TTLs match config (15m upload / 5m download) and are not accidentally longer in any code path.
4. `object_verifier` uses exact content-type matching (regression-tested in Phase 2, re-verify here).
5. AV scan is mandatory in any non-dev environment (`AV_SCAN_ENABLED=true` enforced by deploy config, not just a default).
6. SSE-S3/KMS encryption confirmed enabled on `graphgpt-files`.
7. Outbox/Kafka: confirm no upload-path request ever blocks on a Kafka call (load test with Kafka paused).
8. `/complete` idempotency confirmed under concurrent duplicate requests (race test, not just sequential).
9. Every route resolves through `permission_service.assert_access` — a route-inventory test that fails CI if a new route is added without a permission dependency.

**Exit criteria:** Every item above has a passing automated test or a documented manual verification step; no open item.

**LLD refs:** §9.

---

## Phase 12 — Testing Completion

**Goal:** Fill in whatever the per-phase "Exit criteria" tests above didn't already cover, per the strategy in LLD §10.

**Tasks:**
1. Unit test coverage for `key_builder`, `object_verifier`, `lifecycle_service`, `permission_service` (if not already at 100% from earlier phases).
2. Full integration suite runnable via `docker compose -f docker-compose.test.yml up` + `pytest tests/integration`.
3. Contract tests: snapshot schemas for `file.uploaded`, `file.deleted`, `file.processing_retry`, `file.scan_failed` (produced) and `file.processing.started/completed/failed` (consumed) — fails CI on an unreviewed field change.
4. Idempotency test suite consolidated (outbox replay, processing-consumer replay, `/complete` replay) — these were built per-phase; group them here for a single "replay safety" CI job.
5. Load/perf smoke test: N concurrent `initiate` → direct MinIO PUT → `complete` cycles, confirming File Service CPU/memory stay flat regardless of file size (validates the "large-file path never touches File Service compute" claim from HLD §19).

**Exit criteria:** CI pipeline green across unit/integration/contract suites; coverage threshold met (align with whatever the repo's standard is, e.g. `auth-service`'s).

**LLD refs:** §10.

---

## Phase 13 — Deployment & CI/CD

**Goal:** Ship it.

**Tasks:**
1. `Dockerfile` production build (multi-stage, non-root user, matching `auth-service`/`memory-service` conventions).
2. Kubernetes manifests (or Helm chart) for the File Service deployment + HPA, referencing the MinIO Tenant CRD and Kafka/Postgres already used by other GraphGPT services (HLD §5.1, §19).
3. CI pipeline: lint → unit tests → integration tests (spin up docker-compose test stack) → build image → push → deploy to staging.
4. Alembic migration step wired into the deploy pipeline (run-before-rollout, matching how `auth-service`/`memory-service` handle it).
5. Runbook doc (can live alongside this plan or in `docs/`): what to check first if `/health/ready` is failing, how to manually trigger `reconciliation_worker`, how to inspect outbox backlog.

**Exit criteria:** A merge to `main` deploys File Service to staging automatically; manual promotion to prod is a single approved step.

**LLD refs:** HLD §19–§22.

---

## Cross-Phase Notes

* **Nothing in Phases 3–9 should require touching Phase 0–2 code** except adding new config keys or new topic constants — if a later phase needs to change `key_builder.py`'s signature, that's a signal the earlier phase under-specified something and should be revisited, not silently patched around.
* **Every phase that introduces a new Kafka consumer or producer must also add its dedupe/idempotency test in that same phase** — don't defer all of it to Phase 12; Phase 12 only consolidates and closes gaps.
* **AV scanning (Phase 6) is the highest-risk phase** — it's the one place three previously-separate concerns (content-type trust chain, quarantine promotion, `file.uploaded` emission) meet. Budget the most review time there, and land Phase 3 (AV-disabled path) solidly first so Phase 6 is additive rather than a rewrite.
