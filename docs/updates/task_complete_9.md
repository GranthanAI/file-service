# Task Complete 9: Reconciliation Worker (Phase 9)

This document logs the operational safety-net implementation completed for the **Granthan File Service** under **Phase 9 (Reconciliation Worker)** of [docs/IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md).

---

## 1. What Was Done

1. **The worker:** [workers/reconciliation_worker.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/workers/reconciliation_worker.py) — `run_sweep(db)` (docs/LLD.md §4.6 steps 1–2, DB-only, fully testable without Kafka: expire stale `upload_jobs`, fail `UPLOADING` files abandoned past 1h) and `_check_orphan_backlog(db, consumer)` (step 3: bounded MinIO-notification backlog drain, cross-checked against `files`), combined into `run_reconciliation_worker`, a periodic background task.
2. **New repository method:** `FileRepository.list_abandoned_uploading` — a single query (`status = UPLOADING AND created_at < cutoff AND NOT EXISTS (an active upload_jobs row)`), never a bucket listing.
3. **Shared MinIO-event parsing extracted:** moved `_parse_object_created_event` out of `quarantine_service.py` (Phase 6, private) into [utils/minio_events.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/utils/minio_events.py) as `parse_object_created_event`, now used by both `quarantine_service.py` and `reconciliation_worker.py` — the two Phase 6/9 consumers of the same MinIO event shape no longer duplicate (or privately import) the same parsing logic.
4. **Metrics infra started early:** [core/metrics.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/core/metrics.py) — created now (rather than deferred to Phase 10) so `reconciliation_orphans_total` could be a real Prometheus counter, not a placeholder log line Phase 10 would have had to replace. Phase 10's own metrics wiring builds on this same module.
5. **Wiring:** [lifespan.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/lifespan.py) — started unconditionally, like the outbox worker and processing-status consumer.
6. **Tests:**
   * [tests/integration/test_reconciliation_worker.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_reconciliation_worker.py) — the two documented exit-criteria cases (expired `upload_jobs` row flips to `EXPIRED`; abandoned `UPLOADING` file flips to `FAILED`) against real Postgres, plus two guard cases (a recent `UPLOADING` file is left alone; a file with an active `PENDING` job, even if old, is left alone) and the static "never calls `list_objects`/`ListObjectsV2`" check (AST-based, excluding the module's own docstring, which names the forbidden call to explain why it's forbidden).
   * [tests/unit/test_reconciliation_worker.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_reconciliation_worker.py) — the orphan-backlog cross-check logic (unmatched object flagged, matched-but-still-`UPLOADING` flagged, matched-and-settled not flagged, an event inside the grace period left uncommitted for redelivery) against a mocked Kafka consumer — no live broker needed for this logic.

---

## 2. Why It Was Done

* **The MinIO-event parser was promoted from private-in-quarantine_service to a shared `app/utils/minio_events.py` module, not imported across services as a private name:** `reconciliation_worker.py` needed the exact same (bucket, key)-from-S3-event extraction Phase 6's `quarantine_service.py` already had, and reaching across module boundaries to import a leading-underscore "private" function would have been a code smell for something now used by two independent consumers. A neutral, dependency-free `utils/` module is the natural home — matches this codebase's own convention of `app/utils/` housing small, shared, side-effect-free helpers (`checksum.py`, `id_generator.py`).
* **`app/core/metrics.py` was built now, not deferred to Phase 10:** Phase 9's own task list says the orphan-backlog check should emit `reconciliation_orphans_total`, and that's a real Prometheus counter name from docs/HLD.md §21's metric table, not an invented one. Building a throwaway log-line stand-in now and swapping it for a real metric next phase would be strictly more work than building the (small, well-specified) metrics module once and using it immediately — so the whole documented metric set was defined in one pass, with only `reconciliation_orphans_total` actually wired to a call site in this phase; Phase 10 wires the rest.
* **The orphan-backlog check uses manual offset commits (`enable_auto_commit=False`), not the auto-commit pattern the quarantine/processing-status consumers use:** those two consumers act on every message as soon as it's read, so auto-commit is safe for them. The reconciliation worker's grace-period requirement — "don't flag an event as orphaned until it's had a fair chance to be claimed" — needs the *option* to leave a too-fresh message's offset uncommitted so it's redelivered and re-evaluated on a later sweep once enough real time has passed. Auto-commit would silently advance past it and lose that chance forever, defeating the grace period's entire purpose.
* **`ABANDONED_UPLOAD_TIMEOUT` (1 hour) is a Python constant, not a `Settings` field:** unlike `RECONCILIATION_INTERVAL_SECONDS` or the new `RECONCILIATION_ORPHAN_GRACE_SECONDS` (which docs/LLD.md leaves as clear policy knobs), docs/LLD.md §4.6 states this specific number in its own pseudocode ("created_at < now() - 1 hour") as part of the documented algorithm, not as a tunable — so it's encoded as what it is: a specific, spec-given constant, not one more environment variable nobody was asked to add.
* **The "never calls list_objects" test parses the module's AST and drops the docstring before scanning, rather than a plain substring check:** a naive `"list_objects" not in source` check would false-positive on the module's own docstring, which names the forbidden call (`ListObjectsV2`) to explain *why* it's forbidden — the AST approach checks the actual code, which is what the exit criterion is actually about.

---

## 3. Verification

* `uv run pytest`: **154 passed** (99 unit, 55 integration — Phases 0–9 combined; Phase 9 alone added 4 unit tests and 5 integration tests, plus 4 tests relocated to the new `test_minio_events.py`), 0 failures.
* Ran against real, ephemeral **testcontainers** Postgres for the DB-only sweep logic; the orphan-backlog cross-check logic is unit-tested against a mocked consumer (no live Kafka needed for that layer — the Kafka *connectivity* itself is already proven working by Phase 5/7's tests against a real broker, so this phase's tests focus on what's new: the grace-period/commit-offset logic).
* No regressions: reran the full Phase 0–8 suite after wiring the worker into `lifespan.py`.
