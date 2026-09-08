# Task Complete 6: Quarantine / AV Scan Flow (Phase 6)

This document logs the quarantine/AV-scan implementation completed for the **Granthan File Service** under **Phase 6 (Quarantine / AV Scan Flow)** of [docs/IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md).

---

## 1. What Was Done

1. **Target-bucket flip (task 1):** already correct — `upload_service._target_bucket()` (Phase 3) already returns `BUCKET_QUARANTINE` when `AV_SCAN_ENABLED=true`. No change needed; verified by reading the code rather than assuming.
2. **A real Phase 2 bug fixed:** [storage/bucket_bootstrap.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/storage/bucket_bootstrap.py) was wiring the MinIO `ObjectCreated` bucket notification onto `BUCKET_FILES`, not `BUCKET_QUARANTINE` — the bucket uploads actually land in when AV scanning is on. Notifications on `graphgpt-files` would never fire for anything `quarantine_service` needs to react to. Fixed to target `BUCKET_QUARANTINE`, with a comment explaining why.
3. **Topics:** [events/topics.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/events/topics.py) — added `MINIO_OBJECT_EVENTS` (docs/HLD.md §12's example topic name); the actual runtime value is `settings.MINIO_OBJECT_EVENTS_TOPIC` (new config field), since the real topic name is server-side MinIO config this app doesn't control.
4. **Pluggable AV scanner:** [services/av_scanner.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/av_scanner.py) — a `Scanner` protocol, `ClamdScanner` (real `clamd` TCP INSTREAM client, blocking calls offloaded via `asyncio.to_thread`), `NoOpScanner` (always-clean fallback), and `get_scanner()` selecting between them via `settings.CLAMAV_ENABLED`.
5. **Quarantine service — the full flow:** [services/quarantine_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/quarantine_service.py) — `process_object_created` (the core handler: `UPLOADED -> SCANNING`, one `GetObject` read producing magic-byte sniff + SHA-256 checksum + AV verdict, then `_promote_clean_object` or `_reject_infected_object`) plus `run_quarantine_consumer`, a background Kafka consumer for `settings.MINIO_OBJECT_EVENTS_TOPIC`.
6. **New repository method:** `FileRepository.get_by_bucket_and_key` (docs/LLD.md §2.1's `uq_files_bucket_key` makes this lookup unambiguous) — the inbound MinIO event carries a bucket/key, not a `document_id`.
7. **Wiring:** [lifespan.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/lifespan.py) — starts `run_quarantine_consumer` as a background task, but only when `settings.AV_SCAN_ENABLED` (with it off, nothing ever lands in the quarantine bucket, so there'd be nothing to consume).
8. **Local dev infra:** added a `clamav` service to the root [docker-compose.yml](file:///c:/Users/hp/Desktop/Granthan/docker-compose.yml) (optional — only used when `CLAMAV_ENABLED=true`).
9. **Config:** `CLAMAV_ENABLED` / `CLAMAV_HOST` / `CLAMAV_PORT` / `CLAMAV_TIMEOUT_SECONDS`, `MINIO_OBJECT_EVENTS_TOPIC`.
10. **Tests:**
    * [tests/unit/test_av_scanner.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_av_scanner.py) — `ClamdScanner`'s OK/FOUND parsing (mocking `clamd.ClamdNetworkSocket`, no live daemon needed), `NoOpScanner`, and `get_scanner()`'s selection logic.
    * [tests/unit/test_quarantine_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_quarantine_service.py) — the S3-event-parsing pure function, and `_promote_clean_object`/`_reject_infected_object` exercised directly against an in-memory `File` row (real `lifecycle_service.transition` runs, nothing mocked there) — including the Phase 6 exit criterion's explicit regression check that the clean path lands on `UPLOADED`, never `PROCESSING`.
    * [tests/integration/test_quarantine_flow.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_quarantine_flow.py) — the full clean-promotion and infected-rejection paths against real, ephemeral Postgres + MinIO: object actually moves buckets (or is removed), `checksum_sha256` is a real SHA-256 hex digest, exactly one `file.uploaded`/`file.scan_failed` row lands in the outbox, and a redelivered event for an already-processed object is a no-op.

---

## 2. Why It Was Done

* **A deterministic EICAR-string fake `Scanner`, not a live ClamAV container, backs the integration tests:** `ClamdScanner`'s wire protocol is already proven in isolation against a mocked `clamd` client (unit tests). Standing up a real `clamav/clamav` container per test run adds a slow, first-boot signature-database pull and genuine flakiness risk (network-dependent) for marginal additional confidence — the EICAR test string is *itself* designed to be matched by exact byte comparison (that's literally how every real AV engine detects it, ClamAV included), so a fake scanner that does the same exact-match check exercises the identical detection semantics `quarantine_service`'s orchestration logic actually depends on, without coupling every test run to a live daemon. The `docker-compose.yml` `clamav` service and `ClamdScanner` are there for real deployments; a manual/staging verification against the live container is the appropriate place to prove the full stack, not the unit/integration suite.
* **Magic-byte mismatch is logged, not enforced as a third failure branch:** `docs/LLD.md` §4.5/§6.6's flow diagram defines exactly two outcomes after the scan — Clean or Infected — driven solely by the AV verdict. Nothing in the LLD's state machine or sequence diagrams describes a third "content-type spoofed" terminal state. Inventing one (e.g. a new `last_error_code`, a new outbox event) would be scope creep past what's actually specified; sniffing and logging the mismatch (still genuinely useful signal) satisfies "wired in" (Phase 6 task 3) literally without fabricating unshown behavior.
* **`FileRepository.get_by_bucket_and_key` instead of parsing `document_id` out of the object key:** the key format (`{owner_id}/{document_id}/v{n}/original.{ext}`) is an implementation detail of `key_builder.py`; depending on quarantine_service re-parsing it would couple two modules that shouldn't need to agree on string layout when `uq_files_bucket_key` already provides an exact, indexed lookup.
* **The quarantine consumer only starts when `AV_SCAN_ENABLED`:** starting an `AIOKafkaConsumer` against a topic that can never receive a message (nothing ever lands in the quarantine bucket with AV scanning off) would be a live connection with no purpose — and, more importantly, would silently require a running Kafka broker on every local `AV_SCAN_ENABLED=false` boot, which is the dev/test default.

---

## 3. Code Highlights & Explanations

### 3.1 One Read, Three Outputs

Located in: [app/services/quarantine_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/quarantine_service.py)

```python
data = response.read()
...
sniffed_content_type = sniff_content_type(data)
checksum_sha256 = hashlib.sha256(data).hexdigest()
verdict = await scanner.scan(data)
```
* **Why it matters:** the object body is fetched from MinIO exactly once and reused for all three checks — the LLD's own rationale (§3.5) for deferring these checks out of `/complete` entirely was to avoid a second, redundant body read there; this is the one place in the codebase all three prices get paid together, deliberately.

### 3.2 A Repository Bug Fix Fed Straight Into This Phase

Located in: [app/storage/bucket_bootstrap.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/storage/bucket_bootstrap.py)

```python
client.set_bucket_notification(
    settings.BUCKET_QUARANTINE,   # was settings.BUCKET_FILES
    NotificationConfig(...),
)
```
* **Why it matters:** without this fix, `bucket_bootstrap` would silently configure notifications on the wrong bucket in a real deployment — `quarantine_service` would never receive a single event, and every upload with `AV_SCAN_ENABLED=true` would sit in `SCANNING`... except it wouldn't even reach `SCANNING`, since nothing would ever trigger `process_object_created` in the first place, so files would sit in `UPLOADED` in the quarantine bucket forever. This is exactly the class of orphaned-upload scenario `docs/LLD.md`'s reconciliation worker (Phase 9) exists to catch, but catching it there would have masked a wiring bug that's cheaper to fix now.

---

## 4. Verification

* Full suite (Phases 0–6): **106 passed** at the point Phase 6 was completed (before Phase 7 was layered on in the same session — see task_complete_7.md for the final combined count).
* Ran against real, ephemeral **testcontainers** Postgres + MinIO — confirmed real `CopyObject`/`RemoveObject` behavior, not mocked.
* `clamd`'s OK/FOUND response parsing verified against the package's actual documented response shape (`{"stream": (status, signature)}`), mocked at the `clamd.ClamdNetworkSocket` boundary so the test proves `ClamdScanner`'s own parsing logic without needing a live daemon.
