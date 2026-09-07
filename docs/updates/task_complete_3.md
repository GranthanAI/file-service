# Task Complete 3: Upload Flow — Initiate + Complete, AV Scan Disabled (Phase 3)

This document provides a detailed log of the upload orchestration implementation completed for the **Granthan File Service** under **Phase 3 (Upload Flow, AV Scan Disabled)** of [docs/IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md).

---

## 1. What Was Done

1. **Schemas:** [api/schemas/upload.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/schemas/upload.py) — `InitiateUploadRequest` (with a filename path-separator sanitizer), `InitiateUploadResponse`, `CompleteUploadRequest`, `CompleteUploadResponse`, matching [docs/LLD.md §5.1–§5.2](../LLD.md#5-api-specification--payload-definitions) exactly. [api/schemas/common.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/schemas/common.py) — `ErrorResponse`.
2. **Upload Orchestration:** [services/upload_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/upload_service.py) — `initiate_upload` and `complete_upload`, implementing [docs/LLD.md §4.1](../LLD.md#41-upload-orchestration-appservicesupload_servicepy) step-by-step, including the `AV_SCAN_ENABLED` branch (present, only the `false` path exercised this phase per the plan).
3. **Exceptions:** added `PermissionDeniedError` (403), `FileRecordNotFoundError` (404 — deliberately not named `FileNotFoundError` to avoid shadowing Python's builtin OS-level exception), `AlreadyCompletedError` (409), `PresignExpiredError` (410) to [core/exceptions.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/core/exceptions.py), plus `EXCEPTION_STATUS_MAP` and `register_exception_handlers(app)` — the central handler registration [docs/LLD.md §8](../LLD.md#8-error-handling--exception-mapping-appcoreexceptionspy) calls for.
4. **Router:** [api/routers/upload.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/routers/upload.py) — `POST /files/upload/initiate` (201) and `POST /files/{document_id}/complete` (200), wired into [main.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/main.py) alongside the exception handlers.
5. **Auth dependency:** [api/deps.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/deps.py) — `get_current_user_id`, verifying a JWT issued by auth-service and returning just the `sub` claim as a `UUID` (File Service has no local user table, unlike auth-service's own `get_current_user`).
6. **Real bug fix in `app/db/session.py`:** `get_db` never committed (a Phase 0 gap — see [§3.1](#31-get_db-never-committed) below).
7. **Config:** replaced the never-implemented `AUTH_JWT_PUBLIC_KEY_URL` placeholder with `JWT_SECRET_KEY`/`JWT_ALGORITHM` — see [§2](#2-why-it-was-done) for why.
8. **Tests:**
   * [tests/integration/test_upload_flow.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_upload_flow.py) — the full HTTP round trip (initiate → real PUT/POST to MinIO → complete) against a real, ephemeral Postgres + MinIO, plus idempotency, wrong-owner, disallowed-content-type, no-auth, and both failure-path tests from the Phase 3 exit criteria.
   * Extended [tests/integration/conftest.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/conftest.py) with an `app_client` fixture (a real `TestClient` wired via `dependency_overrides` to the ephemeral test infra) and an `auth_headers` fixture (a real signed JWT, not a mocked auth dependency — the actual verification code path gets exercised).

---

## 2. Why It Was Done

* **`FileRecordNotFoundError` instead of the LLD's literal `FileNotFoundError` name:** Python's standard library already defines `FileNotFoundError` as an OS-level exception (raised when opening a missing file on disk). Reusing that exact name for an unrelated "no DB row for this document_id" condition would be a latent footgun — a bare `except FileNotFoundError:` written anywhere in this codebase for the *real* OS exception would silently also catch this domain exception, or vice versa. Same rationale already applied to other naming choices in earlier phases (e.g. keeping MinIO's `ETag` and a real `checksum_sha256` as separate fields).
* **`JWT_SECRET_KEY`/`JWT_ALGORITHM` (HS256, shared secret) instead of the JWKS/public-key design `auth-service`'s own docs describe:** checked `auth-service/app/security/jwt_manager.py` directly rather than trusting its LLD prose — the actual running code signs with `jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)` where `JWT_ALGORITHM=HS256` in its `.env`, a plain shared-secret scheme, not RS256/JWKS. For File Service to actually verify tokens `auth-service` really issues, it has to match what the code does, not what a doc says it does — so `file-service`'s `.env`/`.env.example` mirror `auth-service`'s exact `JWT_SECRET_KEY` value for local dev interoperability.
* **`get_current_user_id` returns only a `UUID`, not a full user object:** File Service has no local `users` table (by design — it's a metadata/storage service, not an identity one), so unlike `auth-service`'s `get_current_user` (which loads a full `User` row), every File Service endpoint only ever needs the caller's id for ownership/permission checks.
* **The `client_checksum` idempotency nuance:** the LLD's step-by-step pseudocode says any non-`UPLOADING` status returns 200 idempotently, but its own API table separately documents a `409 already completed with different terms` case for `/complete`. Reconciling both: a repeat call with no checksum (or a matching one) is the ordinary idempotent case; a repeat call whose `client_checksum` doesn't match the already-recorded `etag` is the caller mistakenly believing a *different* upload completed, which is exactly what `AlreadyCompletedError` is for.
* **Building `EXCEPTION_STATUS_MAP` as an explicit, append-only dict rather than a decorator or a status-code attribute on each exception class:** every exception the map doesn't recognize falls back to `500`, which surfaces a missing mapping loudly (a 500 in tests) instead of silently returning some default client-error code — and the map's own comment states the rule for future phases: add the entry the same phase the exception is introduced, so it can never drift out of sync with what's actually raised.

---

## 3. A Real Bug Found by Testing

### 3.1 `get_db` Never Committed

**Symptom:** the very first end-to-end HTTP test written for this phase would have silently done nothing — every repository call in `upload_service.py` only ever calls `.flush()` (pushes SQL into the current transaction, never commits it), by design, so that a single request's multiple repository calls can share one atomic transaction. That transaction has to actually get committed *somewhere* — and Phase 0's `get_db` (written before any route existed to exercise it) never did.

**Why it wasn't caught earlier:** Phase 1 and Phase 2's tests exercised repositories and storage code directly against test fixtures that manage their own commit/rollback (`db_session`, `migrated_database_url`) — none of them went through the actual `app/api/deps.py::get_db` dependency the way a real HTTP request does, so the gap was invisible until this phase's tests drove requests through the real FastAPI dependency graph.

**Fix:** `get_db` now commits on successful completion of the request and rolls back on any exception (matching `auth-service`'s own `get_db` pattern in `app/api/deps.py`), located in [app/db/session.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/db/session.py).

### 3.2 The Rollback-vs-Intentional-Partial-Commit Conflict

**Symptom:** once `get_db` correctly rolled back on exception, `test_complete_with_wrong_size_returns_422` failed a second assertion: the API correctly returned `422 SizeMismatchError`, but the file's `files.status` in the database was still `UPLOADING`, not `FAILED` as `complete_upload` had explicitly written moments before.

**Root cause:** `complete_upload`'s failure path is `update_status(..., FAILED) ; raise` — writing the `FAILED` state and *then* deliberately re-raising the exception so the registered handler turns it into a `422` response. But `get_db`'s newly-fixed "rollback on any exception" wrapper doesn't know the difference between "an unexpected crash — discard everything" and "an expected domain exception whose partial write should survive." It rolled back the `FAILED` write along with everything else, silently reverting the file to `UPLOADING`.

**Fix:** the failure branch now calls `await db.commit()` explicitly right before re-raising, so the `FAILED` status (and its `last_error_code`) survive even though the request still ends in an error response. This is the one deliberate exception to "always let `get_db` manage the transaction" in the codebase so far, and is commented in place as such.

**Both of these were caught only because the tests exercise the real HTTP layer** (dependency injection, the actual `get_db`, the actual registered exception handlers) rather than calling `upload_service` functions directly with a hand-managed session — exactly the kind of gap that unit-testing the service function in isolation would have missed entirely.

---

## 4. Code Highlights & Explanations

### 4.1 The Explicit Commit-Before-Re-Raise

Located in: [app/services/upload_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/upload_service.py)

```python
except (ObjectNotFoundError, SizeMismatchError, ContentTypeMismatchError) as e:
    await FileRepository.update_status(db, file, FileStatus.FAILED, last_error_code=type(e).__name__)
    await db.commit()
    raise
```
* **Why it matters:** this is the only place in the codebase so far where a service function manages its own commit instead of leaving it entirely to `get_db`. That's deliberate and load-bearing — without it, every failed verification would silently leave the file stuck in `UPLOADING` forever with no record of why it failed, even though the client correctly received a `422`.

### 4.2 Idempotency With a Checksum Escape Hatch

Located in: [app/services/upload_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/upload_service.py)

```python
if file.status != FileStatus.UPLOADING:
    if client_checksum is not None and file.etag is not None and client_checksum != file.etag:
        raise AlreadyCompletedError(document_id)
    return file
```
* **Why it matters:** reconciles two statements in the LLD that read as contradictory in isolation (§4.1's "any non-UPLOADING status returns 200" vs. §5.2's documented 409 case) into one rule that satisfies both: idempotent by default, an error only when the caller's own evidence (`client_checksum`) contradicts what actually got recorded.

### 4.3 JWT Verification Matching What auth-service Actually Signs

Located in: [app/api/deps.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/deps.py)

```python
payload = jwt.decode(
    credentials.credentials,
    settings.JWT_SECRET_KEY,
    algorithms=[settings.JWT_ALGORITHM],
    options={"require": ["sub", "exp"]},
)
return UUID(payload["sub"])
```
* **Why it matters:** no network call, no database lookup — File Service verifies the token entirely locally using the same secret `auth-service` signs with. `options={"require": [...]}` ensures a token missing either claim is rejected outright rather than producing a confusing `KeyError` deeper in the code.

---

## 5. Verification

* `uv run pytest`: **70 passed** (50 unit, 20 integration — 13 from Phases 1–2, 7 new for Phase 3), 0 failures.
* Confirmed via direct `docker exec` against the real `graphgpt-postgres` container that the Phase 1 migration's tables are intact and correct, independent of this machine's known native-Postgres-on-port-5432 conflict (documented in `README.md`) which blocks host-based tooling from reaching the Docker container directly — the full integration suite already exercises the real HTTP → DB → MinIO path end-to-end via `testcontainers`, which binds to a random port and is unaffected by that conflict.
