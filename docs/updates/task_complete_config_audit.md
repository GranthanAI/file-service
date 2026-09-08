# Task Complete: Configuration Audit (No Hardcoded Values)

This document logs a cross-cutting audit completed for the **Granthan File Service**, requested directly (not tied to a specific [docs/IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md) phase number): confirm every operational/tunable value in `app/` is driven by `Settings`/`.env`, not hardcoded in application code.

---

## 1. What Was Done

1. **Audited** every `.py` file under `app/` (routers, services, workers, events, storage, repositories) for magic numbers, hardcoded bucket/topic/consumer-group names, and any other operational value that should be an environment-driven config knob rather than a literal.
2. **New `Settings` fields added** to [core/config.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/core/config.py):
   * `KAFKA_CONSUMER_POLL_TIMEOUT_MS`, `KAFKA_CONSUMER_MAX_RECORDS` — shared by `processing_status_consumer.py` and `quarantine_service.py`'s consumer loops (were hardcoded `1000`/`50` in both places independently).
   * `RECONCILIATION_POLL_TIMEOUT_MS`, `RECONCILIATION_MAX_RECORDS`, `RECONCILIATION_CONSUMER_GROUP_ID` — `reconciliation_worker.py`'s own poll (were hardcoded `1000`/`200`, and an f-string-concatenated `f"{settings.KAFKA_CONSUMER_GROUP_ID}-reconciliation"`).
   * `OUTBOX_BATCH_SIZE` — `outbox_publisher.publish_batch`'s per-cycle row limit (was a hardcoded `limit: int = 100` parameter default).
   * `FILE_LIST_DEFAULT_PAGE_SIZE`, `FILE_LIST_MAX_PAGE_SIZE` — the `GET /files` pagination default/max, previously hardcoded as `20`/`100` independently in three places (`api/routers/files.py`'s `Query(...)`, `services/file_service.py`'s `limit: int = 20`, `repositories/file_repository.py`'s `limit: int = 20`).
   * `QUARANTINE_OBJECT_EXPIRY_DAYS` — `bucket_bootstrap.py`'s quarantine-bucket lifecycle rule (was a hardcoded `Expiration(days=1)`).
3. **Removed a dead hardcoded default:** `FileRepository.list_paginated`'s `limit: int = 20` was never actually reached by any real call path (every caller — `file_service.py`, and every test — always passes `limit` explicitly), so it's now a required keyword-only parameter instead of a second, silently-unused copy of the same number.
4. **Removed a duplicate literal:** `ClamdScanner.__init__`'s `timeout: float = 30.0` default duplicated `settings.CLAMAV_TIMEOUT_SECONDS`'s own default (`30.0`) as a second, independent number that could drift out of sync — now reads `settings.CLAMAV_TIMEOUT_SECONDS` directly as its default.
5. **`.env.example` brought fully up to date** — it was missing every setting added from Phase 6 onward (`CLAMAV_*`, `MINIO_OBJECT_EVENTS_TOPIC`, `QUARANTINE_OBJECT_EXPIRY_DAYS`, `RECONCILIATION_ORPHAN_GRACE_SECONDS`, `HEALTH_CHECK_TIMEOUT_SECONDS`) as well as all five new fields from this pass. It now lists every `Settings` field with no gaps.
6. **Confirmed clean:** no hardcoded secrets, credentials, or API keys exist anywhere in `app/` — `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, and `JWT_SECRET_KEY` are all required `Settings` fields (no default, no fallback literal) pulled from the environment, never inlined.

---

## 2. Why It Was Done

* **The Kafka `getmany(timeout_ms=1000, max_records=50)` call appeared identically in three separate consumer loops** (`processing_status_consumer.py`, `quarantine_service.py`, and — with a different `max_records` — `reconciliation_worker.py`) with no shared source of truth; a future change to polling cadence would have required editing multiple files and hoping they stayed in sync. A single settings field per concern (live consumers vs. the reconciliation sweep, which deliberately uses a larger batch — see the code comment) removes that risk.
* **`FileRepository.list_paginated`'s `limit` was made required, not given a settings-backed default:** repositories in this codebase are documented as a pure persistence layer ("SQLAlchemy queries only, no business logic" — docs/LLD.md §1) and none of the other repository methods import `settings`. Since every real caller (the service layer) already resolves the policy-driven default itself, giving the repository its own second copy of that default — settings-backed or not — would just be redundant, never-exercised code. Requiring the caller to always be explicit is the more correct fix, not a settings field it doesn't need.
* **`OUTBOX_BATCH_SIZE`/pagination defaults use a `None`-sentinel pattern (`limit: int | None = None`) rather than baking `settings.X` directly into the function signature as a default:** a plain `def f(limit: int = settings.OUTBOX_BATCH_SIZE)` only reads `settings` once, at module-import time — if a test or future caller needed to exercise a different value via `unittest.mock.patch.object(settings, ...)`, a baked-in default would already have captured the old value before the patch ever applied. Resolving the default inside the function body instead means every call re-reads the current `settings.OUTBOX_BATCH_SIZE`, which is both more correct and more testable. (The two places this pattern wasn't used — `av_scanner.ClamdScanner.__init__`'s `timeout` and the `GET /files` router's `Query(default=settings.FILE_LIST_DEFAULT_PAGE_SIZE, ...)` — are both FastAPI/constructor signatures evaluated once at app-startup import time in practice, where this staleness risk doesn't materially apply the same way a per-call function default does.)

---

## 3. Verification

* `uv run pytest`: **154 passed** (99 unit, 55 integration — unchanged from before this pass, confirming the refactor is behavior-preserving), 0 failures.
* Confirmed the existing local `.env` (gitignored, untouched) still boots the app correctly with no changes required — every new `Settings` field has a sane default, so nothing is a breaking requirement for existing deployments.
* Manually diffed `.env.example` against every field in `Settings` to confirm no gaps remain in either direction.
