# Task Complete 2: MinIO Storage Layer (Phase 2)

This document provides a detailed log of the object-storage layer implementation completed for the **Granthan File Service** under **Phase 2 (MinIO Storage Layer)** of [docs/IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md).

---

## 1. What Was Done

1. **Exceptions (pulled forward from Phase 3):** [core/exceptions.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/core/exceptions.py) — added `InvalidContentTypeError`, `UploadSizeExceededError`, `ObjectNotFoundError`, `SizeMismatchError`, `ContentTypeMismatchError`, since Phase 2's own storage/validation modules raise them directly and can't be meaningfully unit-tested without them existing.
2. **Key Builder:** [storage/key_builder.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/storage/key_builder.py) — `build_storage_key(owner_id, document_id, version, filename)`, a pure function producing `{owner_id}/{document_id}/v{version}/original.{ext}`.
3. **Presign Wrappers:** [storage/presign.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/storage/presign.py) — `presign_put`, `presign_post_policy`, `presign_get`, built against the real `minio-py` SDK API (verified by introspection, not assumed from the LLD's illustrative pseudocode).
4. **Object Verifier:** [storage/object_verifier.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/storage/object_verifier.py) — `verify_uploaded_object` + `ObjectVerification` dataclass, with the exact normalized content-type comparison from [docs/LLD.md §3.5](../LLD.md#35-object_verifierpy).
5. **Bucket Bootstrap:** [storage/bucket_bootstrap.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/storage/bucket_bootstrap.py) — `bootstrap_buckets(client)`: creates both buckets, enables versioning + SSE-S3 on `graphgpt-files`, applies the 1-day expiry lifecycle rule on `graphgpt-files-quarantine`, and ensures no public bucket policy is attached. Wired into [lifespan.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/lifespan.py) startup.
6. **Validation Modules:** [validation/content_type.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/validation/content_type.py) (`assert_allowed`), [validation/size_limits.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/validation/size_limits.py) (`assert_within_limit`).
7. **Magic-Byte Sniffer (not wired anywhere yet):** [validation/magic_bytes.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/validation/magic_bytes.py) — `sniff_content_type(data)` using `filetype`. Deliberately unused until Phase 6, per [docs/LLD.md §3.5](../LLD.md#35-object_verifierpy)'s trust-chain ordering.
8. **Config addition:** `MINIO_NOTIFICATIONS_ENABLED` (default `false`) — gates the `set_bucket_notification` call, since it requires a `notify_kafka:file-events` target already registered server-side (`mc admin config set`, [docs/HLD.md §12](../HLD.md#12-minio-event-notifications-defense-in-depth)), which is infra setup this app cannot perform itself.
9. **Root infra update:** added `MINIO_KMS_SECRET_KEY` to the `file-minio` service in [../../docker-compose.yml](file:///c:/Users/hp/Desktop/Granthan/docker-compose.yml) — see [§3](#3-a-real-bug-and-two-real-environment-gaps-found-by-testing) below for why this was necessary.
10. **Tests:**
    * [tests/unit/test_key_builder.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_key_builder.py) — pure function, no I/O.
    * [tests/unit/test_object_verifier.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_object_verifier.py) — mocked `Minio` client, including the exact-match regression test (`application/pdf-malicious` must not satisfy `application/pdf`).
    * [tests/unit/test_validation.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_validation.py) — content-type/size-limit checks.
    * [tests/integration/test_storage.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_storage.py) — a real, ephemeral MinIO via `testcontainers`: presigned PUT and POST-policy upload round trips, presigned GET, all three verification-failure paths, and `bootstrap_buckets` idempotency (run twice, no error).
    * Fixed [tests/unit/test_app_bootstrap.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_app_bootstrap.py) — now mocks `bootstrap_buckets` so this Phase-0 smoke test stays infra-independent now that `lifespan()` calls it for real.

---

## 2. Why It Was Done

* **Verifying the `minio-py` API by introspection before writing code:** the LLD's `storage/` code blocks are illustrative pseudocode (some based on the *Go* SDK, e.g. §9's `PresignedPutObject` example), not a literal spec of the Python SDK's actual method signatures. Rather than transcribe the pseudocode and find out at test time that it doesn't match the real library, every signature used in `presign.py`/`bucket_bootstrap.py`/`object_verifier.py` was checked against the installed `minio` package first (`inspect.signature`, real class constructors for `VersioningConfig`, `SSEConfig`, `LifecycleConfig`, `PostPolicy`, etc.).
* **`MINIO_NOTIFICATIONS_ENABLED` flag instead of always attempting `set_bucket_notification`:** `docs/HLD.md §12` documents the Kafka notification target as something registered via `mc admin config set notify_kafka:...` — a separate, server-side, one-time infra step. Calling `set_bucket_notification` before that target exists always fails; gating it behind an explicit flag (default off) means `bootstrap_buckets` stays safely idempotent in every environment that hasn't provisioned that target yet, rather than crashing app startup over an optional defense-in-depth feature.
* **Merging `key`/`Content-Type` into `presign_post_policy`'s returned `form_data`:** discovered through actual testing (see §3) that the MinIO SDK's `presigned_post_policy()` only returns the cryptographic signing fields, not the condition values the caller added. Doing this merge inside `presign.py` means every future caller (Phase 3's `upload_service.py`, and any client consuming the API) gets one correct, ready-to-submit dict — nobody has to independently discover this SDK quirk.
* **Deriving the presigned-POST upload URL from the `client` object, not global `settings`:** the function must produce a URL for whatever MinIO server `client` is actually configured against — using global `settings.MINIO_ENDPOINT` instead silently works in production (where there's only one client) but is wrong whenever a *different* client is used (every test, and conceptually any multi-tenant future), which is exactly the bug the Phase 2 integration tests caught.
* **`delete_bucket_policy` instead of an explicit `Effect: Deny` JSON policy:** MinIO's bucket-policy engine only accepts specific `s3:` action names, not the `s3:*` wildcard a generic AWS IAM-style deny statement would use. Since MinIO buckets are private by default with no policy attached, ensuring no policy exists is both the simplest and the actually-correct way to guarantee "no public access" — and it's the literal SDK equivalent of the `mc anonymous set none` command already used in `docker/minio/init-buckets.sh` and documented in `docs/HLD.md §14`.
* **`MINIO_KMS_SECRET_KEY` added to the dev MinIO:** `docs/HLD.md §6.3` itself says SSE-S3 goes "via MinIO KMS" — this isn't optional in the MinIO version pinned across this project; without a KMS backend configured, `set_bucket_encryption` fails outright with `NotImplemented`. The single-key built-in KMS (`MINIO_KMS_SECRET_KEY=<name>:<base64 32-byte key>`) is MinIO's own documented way to get SSE-S3 working without standing up an external Vault, and is applied identically to both the dev `file-minio` container and the ephemeral `testcontainers` MinIO used in tests — a dev-only key, not a production secret (production uses the distributed MinIO Tenant CRD's own KMS integration per `docs/HLD.md §5.1`).

---

## 3. A Real Bug and Two Real Environment Gaps (Found by Testing)

Writing the code against the LLD's pseudocode was not enough on its own — running the Phase 2 integration tests against real MinIO (via `testcontainers`) surfaced three concrete issues that unit tests with mocks would never have caught:

### 3.1 The Actual Bug: Wrong MinIO Instance

**Symptom:** `test_presign_post_policy_upload_and_verify` failed with `SignatureDoesNotMatch`.

**Root cause:** `presign_post_policy`'s first draft built the upload URL from `settings.MINIO_ENDPOINT` (the app's globally-configured dev MinIO on `localhost:9010`) instead of from the `client` object actually passed into the function (the ephemeral `testcontainers` MinIO on a random port). Because the dev `file-minio` container happened to also be running at the time, the POST silently went to the *wrong, real* MinIO instance — with different credentials — producing a signature mismatch instead of an obvious connection error, which made it initially look like a signing bug rather than a wrong-target bug.

**Fix:** derive the endpoint and TLS setting from `client._base_url` instead of global settings (see [§4.1](#41-deriving-the-upload-url-from-the-client-not-global-settings) below).

### 3.2 Environment Gap: MinIO Requires a KMS for SSE-S3

**Symptom:** `bootstrap_buckets`' `set_bucket_encryption` call failed with `S3Error: NotImplemented — Server side encryption specified but KMS is not configured`.

**Root cause:** modern MinIO does not support "just turn on AES256" without a KMS backend, even for the S3-managed-key variant — confirmed by testing directly against a real MinIO container with and without `MINIO_KMS_SECRET_KEY` set.

**Fix:** added `MINIO_KMS_SECRET_KEY` (MinIO's built-in single-key KMS, no external Vault needed) to both the dev `file-minio` container and the `testcontainers` fixture.

### 3.3 Environment Gap: MinIO Policy Engine Rejects `s3:*`

**Symptom:** `bootstrap_buckets`' policy-application step failed with `S3Error: MalformedPolicy — unsupported action 's3:*'`.

**Root cause:** the first draft wrote a generic AWS-IAM-style explicit-deny policy (`"Effect": "Deny", "Action": ["s3:*"]`) to enforce "no public access" — a pattern that's valid in AWS IAM but which MinIO's bucket-policy engine rejects, since it only recognizes specific `s3:` action names, not the wildcard.

**Fix:** replaced it with `client.delete_bucket_policy(bucket)` — verified idempotent (safe to call even when no policy exists) and semantically identical to `mc anonymous set none`, which is what the design docs actually called for.

**All three were caught only because the integration tests exercise a real MinIO server, not a mock** — this is the concrete payoff of the `testcontainers` approach chosen in Phase 1 and continued here, and is exactly the class of bug the LLD's own Phase 2 exit criteria (an integration test against real/mocked MinIO, not unit tests alone) was designed to catch.

---

## 4. Code Highlights & Explanations

### 4.1 Deriving the Upload URL from the Client, Not Global Settings

Located in: [app/storage/presign.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/storage/presign.py)

```python
form_data = client.presigned_post_policy(policy)
form_data["key"] = key
form_data["Content-Type"] = content_type

scheme = "https" if client._base_url.is_https else "http"
upload_url = f"{scheme}://{client._base_url.host}/{bucket}"
return upload_url, form_data
```
* **Why it matters:** `client._base_url` is a private attribute of the `minio-py` SDK (there's no public accessor for "what endpoint is this client configured for"), but it's the only correct source of truth for the URL this specific client's presigned policy is valid against. Building the URL from anywhere else (global config, a hardcoded default) works by coincidence whenever there's only one MinIO in play, and breaks silently — as it did here — the moment there's more than one.

### 4.2 The Bucket-Policy Fix

Located in: [app/storage/bucket_bootstrap.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/storage/bucket_bootstrap.py)

```python
# No public bucket access (docs/LLD.md §9 security table): MinIO buckets
# are already private by default with no policy attached — this is the
# SDK equivalent of `mc anonymous set none` ...
for bucket in (settings.BUCKET_FILES, settings.BUCKET_QUARANTINE):
    client.delete_bucket_policy(bucket)
```
* **Why it matters:** the instinct to write an explicit "deny everything" policy is reasonable defensive-programming instinct, but it's the wrong mental model for MinIO's bucket policies, which are ALLOW-oriented (you grant specific access; you don't need to explicitly deny what was never granted). The correct fix ended up being *simpler* than the incorrect first attempt, not more complex.

### 4.3 Idempotent SSE-S3 Bootstrap Requires a KMS

Located in: [../../docker-compose.yml](file:///c:/Users/hp/Desktop/Granthan/docker-compose.yml) (`file-minio` service)

```yaml
environment:
  MINIO_ROOT_USER: minioadmin
  MINIO_ROOT_PASSWORD: minioadmin123
  MINIO_KMS_SECRET_KEY: "file-minio-key:SEg942nDjeIcAe34dNb+Dl/Y8QOOeUrpdgKRxTAtEDQ="
```
* **Why it matters:** without this, `bootstrap_buckets` — which runs on *every* app boot per its own idempotent-by-design contract — would crash the app at startup in any environment (including this one) that hadn't separately provisioned a KMS. Since encryption-at-rest is a named requirement in both the HLD and LLD, the fix had to make it actually work, not silently skip it the way notifications are skipped.

---

## 5. Verification

* `uv run pytest`: **63 passed** (50 unit, 13 integration — 6 from Phase 1, 7 new for Phase 2), 0 failures, only a pre-existing third-party `DeprecationWarning` inside `minio-py` itself (`datetime.utcnow()`) remaining.
* Real app boot against the actual dev `file-minio` container (`uv run uvicorn`, not just testcontainers) confirmed `bucket_bootstrap.complete` fires and `/health/live` returns `200`.
* Manually verified via `mc` against the real dev MinIO after boot:
  * `mc ls local/` → both `graphgpt-files` and `graphgpt-files-quarantine` exist.
  * `mc version info local/graphgpt-files` → versioning enabled.
  * `mc encrypt info local/graphgpt-files` → `Auto encryption 'sse-s3' is enabled`.
  * `mc anonymous get local/graphgpt-files` → `private`.
  * `mc ilm rule ls local/graphgpt-files-quarantine` → `expire-unpromoted-quarantine`, enabled, 1 day.
