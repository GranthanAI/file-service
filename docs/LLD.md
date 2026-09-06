# Low-Level Design (LLD) - Granthan File Service

This document provides a detailed, production-grade Low-Level Design (LLD) for the **Granthan File Service**, mapping directly to the directory structure and the guidelines established in [`HLD.md`](./HLD.md). It follows the same structure and depth used by [`auth-service/docs/lld.md`](../../auth-service/docs/lld.md) and [`memory-service/docs/lld.md`](../../memory-service/docs/lld.md) so the two services stay consistent for anyone reading across the codebase.

Stack: **FastAPI**, **SQLAlchemy (async) + PostgreSQL**, **MinIO** (S3-compatible object storage via `minio` / `boto3` SDK), **aiokafka**, **Alembic**, **uv** for dependency management — matching `auth-service` and `memory-service`.

---

## 1. Directory to Module Mapping: Where, Why, and How

```text
file-service/
├── app/
│   ├── api/
│   │   ├── deps.py                    # DI: DB session, current_user (from Auth JWT), pagination params, permission guard
│   │   ├── middleware.py              # CORS, Correlation/Request-ID, request logging, security headers
│   │   ├── routers/                   # API endpoint groups
│   │   │   ├── upload.py              # POST /files/upload/initiate, POST /files/{document_id}/complete
│   │   │   ├── files.py               # GET /files/{document_id}, GET /files (list), DELETE /files/{document_id}
│   │   │   ├── download.py            # GET /files/{document_id}/download
│   │   │   ├── status.py              # GET /files/{document_id}/status
│   │   │   ├── retry.py               # POST /files/{document_id}/retry
│   │   │   ├── permissions.py         # PUT/GET /files/{document_id}/permissions (share/revoke access)
│   │   │   └── health.py              # GET /health/live (process check), GET /health/ready (DB + MinIO ping — Kafka deliberately excluded, see §4.8)
│   │   └── schemas/                   # Pydantic request/response models
│   │       ├── upload.py              # InitiateUploadRequest, InitiateUploadResponse, CompleteUploadRequest
│   │       ├── file.py                # FileResponse, FileListResponse, FileStatusResponse
│   │       ├── permission.py          # GrantPermissionRequest, PermissionResponse
│   │       └── common.py              # ErrorResponse, PaginatedResponse, SuccessResponse
│   ├── services/                      # Business logic / orchestration layer (stateless)
│   │   ├── upload_service.py          # Orchestrates initiate + complete upload flows
│   │   ├── file_service.py            # Get/list metadata, apply permission filters
│   │   ├── download_service.py        # Permission check + presigned GET issuance
│   │   ├── lifecycle_service.py       # Status state machine transitions (UPLOADING→...→DELETED)
│   │   ├── permission_service.py      # Grant/revoke/check file_permissions rows
│   │   ├── retry_service.py           # FAILED→PROCESSING transition + re-publish event
│   │   ├── version_service.py         # New version creation, version listing
│   │   ├── quarantine_service.py      # Coordinates AV scan promote/reject of quarantined objects
│   │   └── reconciliation_service.py  # Cross-checks MinIO objects vs PostgreSQL rows (orphan detection)
│   ├── repositories/                  # Persistence abstraction (SQLAlchemy queries only, no business logic)
│   │   ├── file_repository.py
│   │   ├── upload_job_repository.py
│   │   ├── file_version_repository.py
│   │   ├── file_permission_repository.py
│   │   └── outbox_repository.py
│   ├── models/                        # SQLAlchemy ORM models -> PostgreSQL tables
│   │   ├── file.py
│   │   ├── upload_job.py
│   │   ├── file_version.py
│   │   ├── file_permission.py
│   │   └── outbox.py
│   ├── storage/                       # MinIO / object-storage adapter layer (file-service's analog of auth's security/)
│   │   ├── minio_client.py            # Process-scoped reusable MinIO client (endpoint, access key, secret key, secure=True)
│   │   ├── bucket_bootstrap.py        # Idempotent startup routine: create buckets, enable versioning/locking/SSE, lifecycle rules, event notifications
│   │   ├── key_builder.py             # Deterministic storage_key construction: {owner_id}/{document_id}/v{n}/original.{ext}
│   │   ├── presign.py                 # PresignedPutObject / PresignedPostPolicy / PresignedGetObject wrappers
│   │   └── object_verifier.py         # StatObject-based verification (size, content-type, ETag/checksum) used by /complete
│   ├── validation/                    # Upload validation rules (separate from storage, testable in isolation)
│   │   ├── content_type.py            # MIME allowlist enforcement
│   │   ├── size_limits.py             # Per-plan / global max upload size checks
│   │   └── magic_bytes.py             # Sniffs first bytes to confirm declared content_type isn't spoofed — invoked from quarantine_service during the AV-scan pass (§3.5, §4.5), not from /complete, since /complete only calls StatObject (metadata) and never reads the object body
│   ├── events/                        # Kafka event production
│   │   ├── producer.py                # aiokafka producer wrapper
│   │   ├── topics.py                  # Topic name constants — produced: file.uploaded, file.deleted, file.processing_retry, file.scan_failed;
│   │   │                              #                     consumed: file.processing.started, file.processing.completed, file.processing.failed
│   │   ├── file_events.py             # Pydantic event payload models
│   │   ├── outbox_publisher.py        # Poll-and-publish outbox implementation (shared pattern with auth-service)
│   │   └── processing_status_consumer.py  # Consumes file.processing.started/completed/failed from Retrieval Service (§3.6)
│   ├── db/
│   │   ├── postgres.py                # Async SQLAlchemy engine
│   │   ├── session.py                 # AsyncSession factory / get_db dependency
│   │   ├── base.py                    # Declarative Base, imports all models for Alembic autogenerate
│   │   └── migrations/                # Alembic migration scripts
│   ├── core/
│   │   ├── config.py                  # Pydantic Settings: DB URL, MinIO endpoint/keys/buckets, Kafka brokers, upload limits
│   │   ├── logging.py                 # Structured JSON logging (request_id, document_id, owner_id fields)
│   │   ├── exceptions.py              # Domain exceptions + FastAPI exception handlers → HTTP status mapping
│   │   ├── constants.py               # Default expiries (presign TTL), max versions, allowlisted MIME types
│   │   └── enums.py                   # FileStatus, PermissionLevel, EventType enums
│   ├── workers/
│   │   ├── outbox_worker.py           # Background loop publishing outbox rows to Kafka
│   │   └── reconciliation_worker.py   # Periodic job: orphaned MinIO objects / stuck upload_jobs cleanup
│   ├── utils/
│   │   ├── checksum.py                # SHA-256 / ETag comparison helpers
│   │   └── id_generator.py            # UUIDv7 document_id generation (time-sortable)
│   ├── main.py                        # FastAPI app factory, router registration, middleware wiring
│   └── lifespan.py                    # Startup: DB pool, MinIO client + bucket_bootstrap, Kafka producer, start outbox_worker,
│                                       #          reconciliation_worker, and processing_status_consumer as background tasks. Shutdown: graceful close
├── alembic.ini
├── docker/
│   └── minio/
│       └── init-buckets.sh            # `mc` script: create buckets, apply IAM policy, configure notifications (used in docker-compose bring-up)
├── docs/
│   ├── HLD_demo.md
│   ├── HLD.md
│   └── LLD.md
├── tests/
│   ├── unit/
│   │   ├── test_key_builder.py
│   │   ├── test_object_verifier.py
│   │   ├── test_lifecycle_service.py
│   │   └── test_permission_service.py
│   ├── integration/
│   │   ├── test_upload_flow.py        # Against a real MinIO + Postgres test containers
│   │   ├── test_download_flow.py
│   │   └── test_outbox_publishing.py
│   └── conftest.py
├── Dockerfile
├── docker-compose.yml                 # app + postgres + minio (4-node or single-node) + kafka (reused from conversation-service if present)
├── Makefile
├── pyproject.toml
├── uv.lock
├── .env.example
└── README.md
```

### Why this shape

* **`storage/` mirrors `security/`** from `auth-service` — both are "adapters to a sensitive external capability" (crypto vs. object storage) and are kept free of HTTP/DB concerns so they can be unit-tested with a mocked MinIO client.
* **`validation/` is split out from `storage/`** because content-type/size rules are pure business policy (change when product requirements change) whereas `storage/` is infrastructure plumbing (changes when the MinIO SDK/API changes). Keeping them separate means a policy change never risks touching presigned-URL code.
* **`services/` stays thin orchestration** — each service method composes 1-3 repository calls + 1 storage call + optionally 1 outbox insert, matching the same layering `auth-service` uses for `auth_service.py`.
* **`workers/reconciliation_worker.py`** is the concrete implementation of [HLD §12](./HLD.md#12-minio-event-notifications-defense-in-depth) — it is a separate worker (not a service) because it runs on a timer independent of any HTTP request.

---

## 2. Database Design & Schema Details

PostgreSQL is the source of truth for metadata; MinIO never stores anything queryable, so every field needed for listing/filtering/authorization must live in PostgreSQL.

### 2.1 `files` Table

```sql
CREATE TYPE file_status AS ENUM (
    'UPLOADING', 'UPLOADED', 'SCANNING', 'PROCESSING', 'READY', 'FAILED', 'DELETING', 'DELETED'
);
-- SCANNING exists only when settings.AV_SCAN_ENABLED=true (see §3.6 and §4.5). It is the state while
-- the object sits in the quarantine bucket awaiting the AV scan verdict. A clean scan returns the file
-- to UPLOADED (NOT directly to PROCESSING) and is what triggers publishing `file.uploaded`; PROCESSING
-- is entered separately and only via `file.processing.started` from Retrieval Service (§3.6).

CREATE TABLE files (
    document_id      UUID PRIMARY KEY,                 -- generated in app (UUIDv7), not gen_random_uuid()
    conversation_id  UUID,
    owner_id         UUID NOT NULL,
    filename         VARCHAR(255) NOT NULL,
    content_type     VARCHAR(150) NOT NULL,
    size_bytes       BIGINT NOT NULL CHECK (size_bytes >= 0),
    storage_bucket   VARCHAR(63) NOT NULL,
    storage_key      TEXT NOT NULL,
    etag             TEXT,                               -- raw MinIO ETag, populated on /complete (StatObject)
    checksum_sha256  VARCHAR(64),                         -- true SHA-256 digest, populated once scanned/hashed
    status           file_status NOT NULL DEFAULT 'UPLOADING',
    current_version  INT NOT NULL DEFAULT 1,
    retry_count      INT NOT NULL DEFAULT 0,
    last_error_code    TEXT,
    last_error_message TEXT,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at       TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_files_owner_id ON files(owner_id);
CREATE INDEX idx_files_conversation_id ON files(conversation_id);
CREATE INDEX idx_files_status ON files(status) WHERE status NOT IN ('DELETED');
CREATE UNIQUE INDEX uq_files_bucket_key ON files(storage_bucket, storage_key);
```

* **Why `document_id` is app-generated UUIDv7, not `gen_random_uuid()` (v4):** `document_id` is already the table's `PRIMARY KEY`, so it already has a B-tree index regardless of which UUID version is used — that index is what makes keyset pagination on `GET /files` efficient (`WHERE document_id > :cursor ORDER BY document_id LIMIT :n`), not a property of UUIDv7 itself. What UUIDv7 adds on top is that it is *time-sortable*: because it's generated at request time (§8) and its high bits encode a timestamp, `ORDER BY document_id` also approximates `ORDER BY created_at` — so the same index doubles as a roughly-chronological cursor without needing a second index on `created_at`. A random UUIDv4 primary key would still paginate correctly via the same index, it just wouldn't sort chronologically.
* **Why `idx_files_status` is partial:** the overwhelmingly common query is "files not yet deleted in state X"; excluding `DELETED` keeps the index small as the table grows (deleted rows vastly outnumber live ones over time).
* **Why `uq_files_bucket_key`:** guarantees the MinIO storage key is never accidentally reused across two metadata rows — a defense against key-builder bugs.
* **Why `etag` and `checksum_sha256` are separate columns:** MinIO's `ETag` is **not guaranteed to be an MD5 or SHA-256 of the whole object** — for multipart uploads it is a hash-of-hashes with a `-N` suffix, and it is an implementation detail of the storage backend, not a cryptographic integrity guarantee for the application. `etag` is stored purely as a cheap, storage-native "did the object change" fingerprint (used by `object_verifier.py` at `/complete`). `checksum_sha256` is a real SHA-256 digest computed by streaming the object once — most naturally as a side effect of the AV scan pass (§3.6, §4.5), since that step already reads every byte — and is the field any integrity-sensitive caller (e.g. deduplication, legal hold) should trust. `checksum_sha256` is nullable and stays `NULL` until that scan/hash step completes.
* **Why `retry_count` / `last_error_code` / `last_error_message` live on `files`:** the retry API (§4.4, §5.7) reports `retry_count` and the status API needs a reason when `status = FAILED` (upload verification failure, AV scan rejection, or a `file.processing.failed` event from Retrieval Service — see §3.6). `last_error_message` is optional/nullable and should be treated as a human-readable hint only, never parsed by callers — `last_error_code` is the stable machine-readable field.

### 2.2 `upload_jobs` Table

Tracks an in-flight presigned-upload session so `/complete` can validate against the exact terms the URL was issued under (and so expired/abandoned sessions can be swept by the reconciliation worker).

```sql
CREATE TABLE upload_jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id      UUID NOT NULL REFERENCES files(document_id) ON DELETE CASCADE,
    presign_method    VARCHAR(10) NOT NULL,              -- 'PUT' | 'POST'
    expected_size    BIGINT NOT NULL,
    expected_content_type VARCHAR(150) NOT NULL,
    issued_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at     TIMESTAMP WITH TIME ZONE,
    status           VARCHAR(20) NOT NULL DEFAULT 'PENDING'  -- PENDING | COMPLETED | EXPIRED
);

CREATE INDEX idx_upload_jobs_pending_expiry ON upload_jobs(expires_at) WHERE status = 'PENDING';
```

* **Why a separate table instead of fields on `files`:** a single `document_id` can accumulate multiple upload attempts (client retries the whole initiate step after a timeout); keeping attempts in their own table means `files` never has ambiguous "which attempt succeeded" bookkeeping, and the reconciliation worker can sweep `PENDING` rows past `expires_at` into `EXPIRED` without touching `files` at all.

### 2.3 `file_versions` Table

```sql
CREATE TABLE file_versions (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id    UUID NOT NULL REFERENCES files(document_id) ON DELETE CASCADE,
    version        INT NOT NULL,
    storage_key    TEXT NOT NULL,
    etag           TEXT NOT NULL,
    checksum_sha256 VARCHAR(64),
    size_bytes     BIGINT NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (document_id, version)
);

CREATE INDEX idx_file_versions_document_id ON file_versions(document_id);
```

* `files.current_version` is denormalized onto the parent row purely as a read optimization (avoids a join on every metadata fetch); `file_versions` remains the append-only source of truth.

### 2.4 `file_permissions` Table

**Scope decision: USER-only for the initial implementation.** Team-based sharing requires resolving team membership (either by calling Auth/Identity Service on every permission check, or replicating membership locally and keeping it in sync) — that's real, ongoing complexity that isn't justified unless team sharing is an actual near-term product requirement. `principal_type` is kept in the schema as a forward-compatible column so adding `TEAM` later is a data/service change, not a migration, but **the application only ever writes/reads `principal_type = 'USER'` today**, and `permission_service.assert_access` (§4.3) contains no team-resolution logic.

```sql
CREATE TYPE permission_level AS ENUM ('VIEWER', 'EDITOR', 'OWNER');

CREATE TABLE file_permissions (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id    UUID NOT NULL REFERENCES files(document_id) ON DELETE CASCADE,
    principal_id   UUID NOT NULL,                 -- user_id today; team_id reserved for future use
    principal_type VARCHAR(10) NOT NULL DEFAULT 'USER',  -- 'USER' only for now; 'TEAM' reserved, not implemented
    access_level   permission_level NOT NULL,
    granted_by     UUID NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (document_id, principal_id, principal_type)
);

CREATE INDEX idx_file_permissions_principal ON file_permissions(principal_id, principal_type);
```

* The owner (`files.owner_id`) implicitly has `OWNER` access and does **not** need a row here — this table only holds *shared* access grants, keeping it small relative to the number of files.
* If team sharing becomes a real requirement later, the addition is additive: implement a `principal_type = 'TEAM'` branch in `permission_service.assert_access` that expands a `principal_id` (team) into its member user IDs (via a call to Auth/Identity Service, likely cached), without touching this table's shape.

### 2.5 `outbox_events` Table (Transactional Outbox Pattern)

```sql
CREATE TABLE outbox_events (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,                    -- document_id
    event_type   VARCHAR(100) NOT NULL,             -- file.uploaded | file.deleted | file.processing_retry | file.scan_failed
    payload      JSONB NOT NULL,
    processed    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_unprocessed ON outbox_events(created_at) WHERE processed = FALSE;
```

Identical pattern to `auth-service`'s `outbox` table — see [HLD §13](./HLD.md#13-reliable-event-publishing-transactional-outbox) for the rationale (avoids the dual-write problem between PostgreSQL and Kafka).

### 2.6 Entity Relationship Summary

```mermaid
erDiagram
    files ||--o{ file_versions : "has"
    files ||--o{ file_permissions : "shared via"
    files ||--o{ upload_jobs : "attempted via"
    files ||--o{ outbox_events : "emits (by aggregate_id)"

    files {
        uuid document_id PK
        uuid conversation_id
        uuid owner_id
        varchar filename
        varchar content_type
        bigint size_bytes
        varchar storage_bucket
        text storage_key
        text etag
        varchar checksum_sha256
        enum status
        int current_version
        int retry_count
    }
    file_versions {
        uuid id PK
        uuid document_id FK
        int version
        text storage_key
        text etag
        varchar checksum_sha256
    }
    file_permissions {
        uuid id PK
        uuid document_id FK
        uuid principal_id
        enum access_level
    }
    upload_jobs {
        uuid id PK
        uuid document_id FK
        varchar status
        timestamptz expires_at
    }
    outbox_events {
        uuid id PK
        uuid aggregate_id
        varchar event_type
        jsonb payload
        bool processed
    }
```

---

## 3. Object Storage Layer (`app/storage/`) — Detailed Design

### 3.1 `minio_client.py`

```python
from minio import Minio
from app.core.config import settings

_client: Minio | None = None

def get_minio_client() -> Minio:
    global _client
    if _client is None:
        _client = Minio(
            endpoint=settings.MINIO_ENDPOINT,          # e.g. "minio.internal:9000"
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_USE_TLS,
        )
    return _client
```

This is a **process-scoped reusable client**, not a globally-singleton one — with multiple Uvicorn/Gunicorn worker processes per pod, each worker process constructs and holds its own `Minio` instance (`Pod → worker 1 → client`, `worker 2 → client`, ...); that's expected and fine, since MinIO client objects are cheap and hold no shared state across processes. Within one worker process, the client is reused across requests (the underlying `urllib3` pool handles connection reuse) — it is constructed once during `lifespan.py` startup and stored on `app.state`, not re-created per request.

### 3.2 `bucket_bootstrap.py`

Runs once at startup (idempotent — safe to run on every pod boot):

```python
def bootstrap_buckets(client: Minio) -> None:
    for bucket in (settings.BUCKET_FILES, settings.BUCKET_QUARANTINE):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

    client.set_bucket_versioning(settings.BUCKET_FILES, VersioningConfig(ENABLED))

    client.set_bucket_encryption(
        settings.BUCKET_FILES,
        SSEConfig(Rule.new_sse_s3_rule()),
    )

    client.set_bucket_lifecycle(
        settings.BUCKET_QUARANTINE,
        LifecycleConfig([
            Rule(
                status=ENABLED,
                rule_filter=Filter(prefix=""),
                rule_id="expire-unpromoted-quarantine",
                expiration=Expiration(days=1),
            ),
        ]),
    )

    client.set_bucket_policy(settings.BUCKET_FILES, DENY_PUBLIC_POLICY_JSON)

    client.set_bucket_notification(
        settings.BUCKET_FILES,
        NotificationConfig(queue_config_list=[
            QueueConfig(
                queue_arn=settings.MINIO_KAFKA_NOTIFICATION_ARN,
                events=["s3:ObjectCreated:*"],
            ),
        ]),
    )
```

* **Why idempotent-on-boot rather than a one-off migration script:** MinIO bucket config is infrastructure state, not application data — treating it like Alembic migrations (versioned, one-shot) would require a second migration system. Re-applying the same desired-state config on every boot is cheap and self-healing if config drifts (e.g., someone manually changes a bucket policy via the Console).

### 3.3 `key_builder.py`

```python
def build_storage_key(owner_id: UUID, document_id: UUID, version: int, filename: str) -> str:
    ext = Path(filename).suffix.lstrip(".") or "bin"
    return f"{owner_id}/{document_id}/v{version}/original.{ext}"
```

Pure function, fully unit-testable without any MinIO/DB dependency — this is why it lives in its own module rather than inline in `upload_service.py`.

### 3.4 `presign.py`

```python
def presign_put(client: Minio, bucket: str, key: str, expiry: timedelta) -> str:
    return client.presigned_put_object(bucket, key, expires=expiry)

def presign_post_policy(client: Minio, bucket: str, key: str, content_type: str,
                         max_size: int, expiry: timedelta) -> tuple[str, dict]:
    policy = PostPolicy(bucket, expiration=datetime.utcnow() + expiry)
    policy.add_equals_condition("key", key)
    policy.add_equals_condition("Content-Type", content_type)
    policy.add_content_length_range_condition(1, max_size)
    return client.presigned_post_policy(policy)

def presign_get(client: Minio, bucket: str, key: str, expiry: timedelta,
                 response_filename: str | None = None) -> str:
    extra_headers = None
    if response_filename:
        extra_headers = {"response-content-disposition": f'attachment; filename="{response_filename}"'}
    return client.presigned_get_object(bucket, key, expires=expiry, response_headers=extra_headers)
```

`upload_service.py` chooses PUT vs. POST-policy based on `settings.UPLOAD_STRICT_POLICY` — POST-policy is preferred whenever content-type/size must be enforced by MinIO itself (defense-in-depth beyond app-layer validation).

### 3.5 `object_verifier.py`

```python
def _normalize_content_type(content_type: str) -> str:
    # Strip parameters (e.g. "; charset=utf-8") and normalize case/whitespace —
    # exact match only, never prefix/substring matching (a prefix check would let
    # "application/pdf-malicious" pass against an expected "application/pdf").
    return content_type.split(";")[0].strip().lower()

def verify_uploaded_object(client: Minio, bucket: str, key: str,
                            expected_size: int, expected_content_type: str) -> ObjectVerification:
    try:
        stat = client.stat_object(bucket, key)
    except S3Error as e:
        if e.code == "NoSuchKey":
            raise ObjectNotFoundError(bucket, key)
        raise

    if stat.size != expected_size:
        raise SizeMismatchError(expected_size, stat.size)

    actual_ct = _normalize_content_type(stat.content_type or "")
    expected_ct = _normalize_content_type(expected_content_type)
    if actual_ct != expected_ct:
        raise ContentTypeMismatchError(expected_ct, actual_ct)

    return ObjectVerification(etag=stat.etag, size=stat.size, content_type=actual_ct)
```

Called exclusively from `upload_service.complete_upload()` — this is the **first** trust boundary, where the service stops believing client-declared metadata and starts believing MinIO's own record of what bytes actually landed. It is deliberately an **exact, normalized string comparison**, not a prefix/substring check — a prefix check (`actual.startswith(expected)`) would incorrectly accept `application/pdf-malicious` against an expected `application/pdf`.

`object_verifier.py` only proves the declared content-type matches what MinIO recorded as the HTTP `Content-Type` on upload — that header is still client-supplied at PUT time and is **not** proof of the file's real format. The full trust chain (see §3.6) layers three more checks after this one before a file is ever considered safe for downstream processing:

```text
1. declared MIME (Content-Type header)     — checked here, object_verifier.py, at /complete
2. magic-byte sniffing (validation/magic_bytes.py)  — checked during the AV-scan pass (§4.5), because
                                                        it requires reading the object's actual bytes,
                                                        which /complete deliberately avoids doing (StatObject
                                                        only reads metadata, not the object body)
3. file-parser validation                  — performed by Retrieval Service when it actually parses the
                                                        document (a corrupt/malformed PDF fails there, not here)
4. AV scan                                 — validation/quarantine_service.py, §4.5
```

File Service is only responsible for steps 1 and 2 (declared MIME + magic bytes) and for orchestrating step 4 (AV scan); step 3 is intentionally Retrieval Service's responsibility, consistent with the processing-ownership boundary defined below.

### 3.6 Processing Status Contract (File Service ↔ Retrieval Service)

The current GraphGPT architecture does **not** have the File Service perform parsing/chunking/embedding — that is owned entirely by **Retrieval Service**:

```text
File Service
      │
      │ file.uploaded
      ▼
Retrieval Service
      │
      ├── Document Parser
      ├── Chunker
      ├── Embedding
      └── Milvus
```

This means `files.status` moving through `PROCESSING → READY` (or `→ FAILED`) is driven by **events Retrieval Service publishes back**, not by anything File Service computes internally. The LLD previously left this boundary implicit (a `lifecycle_service` existed, but nothing described who calls it after `file.uploaded`). The explicit contract is:

```text
file.uploaded                         (File Service → Retrieval Service, outbox-published)
      ↓
Retrieval Service starts parsing
      ↓
file.processing.started               (Retrieval Service → File Service)
      ↓                                   File Service: UPLOADED → PROCESSING
Retrieval Service parses/chunks/embeds/writes to Milvus
      ↓
   ┌──────────────┴───────────────┐
   ▼                              ▼
file.processing.completed   file.processing.failed
   ↓                              ↓
File Service: PROCESSING → READY   File Service: PROCESSING → FAILED
                                    (last_error_code/last_error_message set from event payload)
```

**Ownership split:** File Service owns and is the only writer of `files.status`; Retrieval Service owns the actual parse/chunk/embed work and never touches PostgreSQL rows in `files` — it only ever emits these three events. This keeps the two services' databases fully independent.

#### Event payloads (consumed by File Service)

```json
// file.processing.started
{ "event_id": "evt_201", "document_id": "018f2f3a-...", "timestamp": "..." }

// file.processing.completed
{ "event_id": "evt_202", "document_id": "018f2f3a-...", "chunk_count": 42, "timestamp": "..." }

// file.processing.failed
{ "event_id": "evt_203", "document_id": "018f2f3a-...", "error_code": "PARSE_ERROR",
  "error_message": "Unsupported PDF encoding", "timestamp": "..." }
```

#### Consumer implementation (`app/events/processing_status_consumer.py`)

A new module, added alongside `outbox_publisher.py` under `app/events/`, and run as a background consumer task from `lifespan.py` (mirrors how `outbox_worker.py` runs as a background task):

```text
consume topic "file.processing.started"   → lifecycle_service.transition(UPLOADED -> PROCESSING); file_repository.update_status(...)
consume topic "file.processing.completed" → lifecycle_service.transition(PROCESSING -> READY); file_repository.update_status(...)
consume topic "file.processing.failed"    → lifecycle_service.transition(PROCESSING -> FAILED);
                                             file_repository.update_status(..., last_error_code, last_error_message)
```

Every handler is **idempotent on `event_id`** (see §3.7) — e.g. a duplicate `file.processing.completed` for a file already `READY` is a no-op, not an error, since `lifecycle_service.transition` treats `READY -> READY` as already-satisfied rather than an illegal transition when the target equals the current state.

The retry flow (§4.4) closes the loop this raised: after `POST /files/{document_id}/retry` moves a file back to `PROCESSING` and republishes for Retrieval Service, the file only reaches `READY` again once Retrieval Service emits a fresh `file.processing.completed` — retry never sets `READY` itself.

### 3.7 Kafka Delivery Semantics

**Kafka delivery in this system is at-least-once, not exactly-once, in both directions** (File Service → Retrieval Service via `file.uploaded`, and Retrieval Service → File Service via the `file.processing.*` events in §3.6). This follows directly from the outbox-publish crash window described in §4.7 — a publish can succeed and still get replayed if the worker crashes before marking the outbox row `processed = TRUE`.

Consequences, stated explicitly here rather than left implicit:

* **Every event carries a unique `event_id`.** Any consumer — Retrieval Service consuming `file.uploaded`, or File Service's `processing_status_consumer.py` consuming `file.processing.*` — must dedupe on `event_id` (e.g. an `INSERT ... ON CONFLICT DO NOTHING` into a small `processed_event_ids` table, or an idempotent upsert keyed on `event_id`) before acting on a message.
* File Service's own handlers in §3.6 are written to be safe under replay: re-applying `file.processing.completed` to a file already `READY` is a no-op; re-applying `file.processing.failed` just re-writes the same `last_error_code`.
* This is documented here as a hard requirement for **any future consumer** added to either service's event contract — "Kafka delivered it once" must never be assumed.

---

## 4. Internal Components & Modules

### 4.1 Upload Orchestration (`app/services/upload_service.py`)

**`initiate_upload(owner_id, conversation_id, filename, content_type, size_bytes) -> InitiateUploadResult`**

```text
1. validation.content_type.assert_allowed(content_type)
2. validation.size_limits.assert_within_limit(size_bytes, owner_id)
3. document_id = utils.id_generator.new_uuid7()
4. storage_key = storage.key_builder.build_storage_key(owner_id, document_id, version=1, filename)
5. repositories.file_repository.insert(
     document_id, conversation_id, owner_id, filename, content_type,
     size_bytes, bucket=settings.BUCKET_FILES, storage_key, status=UPLOADING
   )
6. repositories.upload_job_repository.insert(document_id, expected_size=size_bytes,
     expected_content_type=content_type, expires_at=now()+15m)
7. url (+ form_data if POST policy) = storage.presign.presign_put_or_post(...)
8. return InitiateUploadResult(document_id, url, form_data, expires_in=900)
```

Steps 5-6 run inside a single DB transaction (both rows or neither) — an `upload_jobs` row without a matching `files` row (or vice versa) would break the `/complete` lookup.

**`complete_upload(document_id, requester_id) -> FileResponse`**

This is the **one authoritative `/complete` implementation** — it behaves identically whether `AV_SCAN_ENABLED` is true or false; only the terminal state and whether `file.uploaded` is emitted here differ, and that branch is explicit rather than two competing flows:

```text
1. file = file_repository.get_for_update(document_id)          -- SELECT ... FOR UPDATE
2. assert file.owner_id == requester_id                         -- 403 otherwise
3. assert file.status == UPLOADING                               -- idempotency: if already UPLOADED/SCANNING/READY, return current state (200, no side effects)
4. job = upload_job_repository.get_pending(document_id)
5. assert now() < job.expires_at                                  -- else 410 Gone, caller must re-initiate
6. verification = storage.object_verifier.verify_uploaded_object(
      bucket=file.storage_bucket, key=file.storage_key,             -- storage_bucket is ALWAYS the
      expected_size=job.expected_size, expected_content_type=job.expected_content_type)  -- quarantine bucket
                                                                                            -- when AV_SCAN_ENABLED
7. BEGIN TRANSACTION:
     file_version_repository.insert(document_id, version=1, storage_key=file.storage_key,
        etag=verification.etag, size_bytes=verification.size)
     upload_job_repository.mark_completed(job.id)
     if settings.AV_SCAN_ENABLED:
         file_repository.update_status(document_id, status=UPLOADED, etag=verification.etag)
         # no outbox row here — file.uploaded is NOT emitted yet; quarantine_service (§4.5)
         # emits it only after the object is promoted out of the quarantine bucket
     else:
         file_repository.update_status(document_id, status=UPLOADED, etag=verification.etag)
         outbox_repository.insert(aggregate_id=document_id, event_type="file.uploaded",
            payload=build_file_uploaded_payload(file, verification))
   COMMIT
8. return FileResponse.from_model(file)
```

**`file.uploaded` always means "bytes are verified and safe for downstream processing," never merely "bytes landed in some bucket."** In the `AV_SCAN_ENABLED=true` path (the production default), reaching `UPLOADED` here is an intermediate state — the object still lives in `graphgpt-files-quarantine`, and `quarantine_service` (§4.5) is the only code path that ever writes the `file.uploaded` outbox row. In the `AV_SCAN_ENABLED=false` path (local/dev only), `complete_upload` targets `graphgpt-files` directly and is itself the terminal step.

If step 6 raises `ObjectNotFoundError` / `SizeMismatchError` / `ContentTypeMismatchError`, the handler catches it, transitions `files.status → FAILED` (with `last_error_code` set), and returns `422 Unprocessable Entity` with the specific reason — no outbox event is written, so no downstream processing is ever triggered for an unverified upload.

### 4.2 Lifecycle State Machine (`app/services/lifecycle_service.py`)

Enforced centrally so no other module writes `files.status` directly:

```python
ALLOWED_TRANSITIONS = {
    FileStatus.UPLOADING:  {FileStatus.UPLOADED, FileStatus.FAILED},
    # UPLOADED is reached twice on the AV_SCAN_ENABLED=true path: once right after /complete (object still
    # in quarantine), and again after a clean scan promotes the object — both times it means the same thing,
    # "verified bytes, not yet claimed by Retrieval Service." quarantine_service moves UPLOADED -> SCANNING
    # itself as soon as it picks up the ObjectCreated event.
    FileStatus.UPLOADED:   {FileStatus.SCANNING, FileStatus.PROCESSING, FileStatus.FAILED},
    FileStatus.SCANNING:   {FileStatus.UPLOADED, FileStatus.FAILED},      # clean scan returns to UPLOADED, which is what triggers publishing file.uploaded (§4.5) — it never jumps straight to PROCESSING
    FileStatus.PROCESSING: {FileStatus.READY, FileStatus.FAILED},        # entered only via file.processing.started (§3.6) — never set internally by upload/quarantine code
    FileStatus.READY:      {FileStatus.DELETING, FileStatus.PROCESSING},  # re-processing (e.g. reindex)
    FileStatus.FAILED:     {FileStatus.PROCESSING, FileStatus.DELETING},
    FileStatus.DELETING:   {FileStatus.DELETED, FileStatus.FAILED},       # deletion itself can fail (retention lock)
}

def transition(current: FileStatus, target: FileStatus) -> None:
    if current == target:
        return  # idempotent no-op — safe under Kafka at-least-once redelivery (§3.7)
    if target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise InvalidStateTransitionError(current, target)
```

Every repository call that changes `status` goes through `transition()` first — this is what makes the state machine in [HLD §11](./HLD.md#11-file-lifecycle--processing-status) actually enforced in code rather than just documented. Note that **File Service code never calls `transition(UPLOADED, PROCESSING)` or `transition(PROCESSING, READY)` on its own initiative** — both edges are only ever taken from inside `processing_status_consumer.py` (§3.6), in response to `file.processing.started` and `file.processing.completed` respectively, since Retrieval Service — not File Service, and not `quarantine_service` — is the one that knows when processing actually starts or finishes. `upload_service` and `quarantine_service` only ever drive the machine as far as `UPLOADED` (directly, or via `SCANNING`); crossing into `PROCESSING` always happens on the consumer side.

### 4.3 Permission Enforcement (`app/services/permission_service.py`)

```python
async def assert_access(document_id: UUID, principal_id: UUID, required: PermissionLevel) -> None:
    file = await file_repository.get(document_id)
    if file.owner_id == principal_id:
        return  # owner always has full access
    grant = await file_permission_repository.get(document_id, principal_id)
    if grant is None or LEVEL_RANK[grant.access_level] < LEVEL_RANK[required]:
        raise PermissionDeniedError(document_id, principal_id, required)
```

`LEVEL_RANK = {VIEWER: 1, EDITOR: 2, OWNER: 3}` — a simple total ordering avoids a permission-matrix table. `file_permission_repository.get(document_id, principal_id)` looks up a direct `USER` grant only (see §2.4 for why team resolution is deliberately not implemented here yet). Called from `api/deps.py` as a FastAPI dependency (`require_permission(PermissionLevel.VIEWER)`) so routers declare the requirement declaratively:

```python
@router.get("/files/{document_id}/download")
async def download(document_id: UUID, _: None = Depends(require_permission(PermissionLevel.VIEWER))):
    ...
```

### 4.4 Retry (`app/services/retry_service.py`)

```text
1. file = file_repository.get(document_id)
2. assert file.status == FAILED                     -- else 409 Conflict
3. lifecycle_service.transition(FAILED -> PROCESSING)
4. file_repository.update_status(document_id, PROCESSING)
5. outbox_repository.insert(aggregate_id=document_id, event_type="file.processing_retry",
     payload={"document_id": ..., "retry_count": file.retry_count + 1})
6. file_repository.increment_retry_count(document_id)
```

The original object in MinIO is untouched — retry only re-signals Retrieval Service to re-attempt processing. **`retry_service` never sets `READY`** — after step 6, the file sits in `PROCESSING` until Retrieval Service consumes `file.processing_retry` and eventually emits `file.processing.completed` or `file.processing.failed` (§3.6), exactly like the original `file.uploaded`-triggered processing path. This closes the gap in the original draft, where retry re-entered `PROCESSING` but nothing ever described who was responsible for moving it back out.

### 4.5 Quarantine / Malware Scan (`app/services/quarantine_service.py`) — authoritative flow

This is the **single flow** for how bytes go from "landed in MinIO" to `file.uploaded`; there is no second, competing path. It implements [HLD §14](./HLD.md#14-security)'s quarantine requirement as an event-driven consumer (not a synchronous part of `/complete`, so AV scan latency never blocks the upload API):

```text
INITIATE                              upload_service.initiate_upload — storage_key targets BUCKET_QUARANTINE
   ↓
Client PUT to quarantine bucket (direct, presigned URL)
   ↓
COMPLETE                              upload_service.complete_upload — StatObject against quarantine bucket
   ↓
files.status = UPLOADED               (still in quarantine bucket — NOT yet visible to Retrieval Service)
   ↓
MinIO ObjectCreated (quarantine bucket) → Kafka → quarantine_service (AV Scan Consumer)
   ↓
lifecycle_service.transition(UPLOADED -> SCANNING)
   ↓
AV scan streams the object body — this is also where validation/magic_bytes.py runs (§3.5) and,
optionally, checksum_sha256 is computed as a side effect of the same read
   │
   ├── Infected ─────────────────────────────────────────────┐
   │                                                          ▼
   │                                        RemoveObject(quarantine)
   │                                        UPDATE files SET status=FAILED,
   │                                            last_error_code='AV_SCAN_INFECTED'
   │                                        outbox: file.scan_failed
   │                                        (file.uploaded is NEVER emitted for this document_id)
   │
   └── Clean
                     ↓
        CopyObject(quarantine bucket → graphgpt-files, same key)
                     ↓
        UPDATE files SET storage_bucket='graphgpt-files', checksum_sha256=<computed>
                     ↓
        RemoveObject(quarantine)
                     ↓
        lifecycle_service.transition(SCANNING -> UPLOADED)   -- NOT -> PROCESSING; see note below
                     ↓
        outbox: file.uploaded                 ← this is the ONLY place file.uploaded is ever written
                     ↓                            when AV_SCAN_ENABLED=true
        Retrieval Service consumes file.uploaded, starts parsing, emits file.processing.started
                     ↓
        File Service: processing_status_consumer.py transitions UPLOADED -> PROCESSING (§3.6)
```

**Why the clean-scan path lands on `UPLOADED`, not `PROCESSING`:** the transition into `PROCESSING` is defined once, exclusively, as the effect of consuming `file.processing.started` (§3.6) — that is the single place in the codebase that makes the claim "processing has actually started," and it belongs to `processing_status_consumer.py`, not to `quarantine_service`. If `quarantine_service` set `PROCESSING` directly, the state would claim processing had started before Retrieval Service had even consumed the event, and a later `file.processing.started` would have nothing to transition *from* (since `PROCESSING → PROCESSING` on `UPLOADED`'s own promotion would already have happened for the wrong reason). Landing on `UPLOADED` keeps a single writer for the `UPLOADED → PROCESSING` edge regardless of whether the file went through quarantine or not — the two callers of `complete_upload`/`quarantine_service` never race with `processing_status_consumer.py` over the same edge.

When `AV_SCAN_ENABLED=false` (local/dev only — never production), this entire module is bypassed: `complete_upload` targets `graphgpt-files` directly and is itself the sole place `file.uploaded` is emitted (as shown in §4.1), landing the file on `UPLOADED` exactly as the clean-scan path above does. Exactly one of `upload_service.complete_upload` or `quarantine_service` ever writes the `file.uploaded` outbox row for a given `document_id`, controlled by the single `AV_SCAN_ENABLED` flag — there is no scenario where both, or neither, could fire — and in both cases the file sits in `UPLOADED` until `processing_status_consumer.py` moves it into `PROCESSING`.

### 4.6 Reconciliation Worker (`app/workers/reconciliation_worker.py`)

Runs every N minutes (`settings.RECONCILIATION_INTERVAL_SECONDS`, default 300):

```text
1. Expire stale upload_jobs: UPDATE upload_jobs SET status='EXPIRED'
   WHERE status='PENDING' AND expires_at < now()
2. For files still in UPLOADING with no PENDING/COMPLETED upload_job and
   created_at < now() - 1 hour: transition -> FAILED (abandoned upload)
3. Consume backlog from `minio.object.events` (MinIO bucket notification topic, HLD §12):
   for each ObjectCreated event, check a matching `files` row with status != UPLOADING exists;
   if not found within a grace period -> alert (metric: reconciliation_orphans_total) for manual review
```

**Explicitly out of scope: this worker never issues a `ListObjectsV2`/full-bucket `LIST` against MinIO.** At scale (millions of objects), a periodic full-bucket scan is expensive and grows unbounded with bucket size, while everything reconciliation actually needs — expired sessions, abandoned uploads, orphaned objects — is already derivable from (a) `upload_jobs` expiry (bounded by the presign TTL), (b) `files` rows stuck in a transient state past a timeout, and (c) the MinIO event stream (bounded by upload rate, not bucket size). If a *targeted* check against a specific key is ever needed (e.g. investigating one flagged `document_id`), that's a single `StatObject` call, not a bucket listing.

### 4.7 Outbox Worker (`app/workers/outbox_worker.py`)

Identical pattern and polling query to `auth-service`:

```sql
SELECT * FROM outbox_events
WHERE processed = FALSE
ORDER BY created_at ASC
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

For each row: publish to the Kafka topic named in `event_type` (via `events/topics.py` mapping), then on ACK set `processed = TRUE`. `FOR UPDATE SKIP LOCKED` allows multiple File Service pods to run the worker concurrently without double-publishing the same row — **but it does not make publishing exactly-once.** If the process crashes between the Kafka ACK and the `UPDATE processed = TRUE` commit, the row is still picked up by the next poll and republished. This is by design (favoring at-least-once over risking a dropped event) — see §3.7 for the consumer-side idempotency requirement this creates.

### 4.8 Health Checks (`app/api/routers/health.py`)

Liveness and readiness are deliberately different checks, because one of this service's core strengths (§9 in `HLD.md`, "Kafka unavailable → outbox/retry") would otherwise be silently defeated by the health endpoint used to gate traffic/restarts:

```text
GET /health/live
    -> 200 if the process can respond at all (no dependency calls). Used by the orchestrator to decide
       whether to restart the pod. A slow DB/MinIO/Kafka must never make this fail — that would cause
       restart-loops that fix nothing.

GET /health/ready
    -> 200 only if:
         - PostgreSQL responds to `SELECT 1` within timeout, AND
         - MinIO responds to a lightweight call (e.g. `bucket_exists(BUCKET_FILES)`) within timeout
    -> Kafka is intentionally NOT part of this check.
```

**Why Kafka is excluded from readiness:** §13/§4.1 establish that upload `initiate`/`complete` never touch Kafka directly — they only write to `outbox_events` inside the same PostgreSQL transaction as the metadata change. A File Service pod with Postgres and MinIO up but Kafka down can still accept uploads end-to-end; the outbox simply queues until `outbox_worker.py` can reconnect and drain it. If Kafka were wired into `/health/ready`, a Kafka outage would flip every pod `NotReady`, removing all upload capacity from the load balancer for a dependency that the architecture was specifically designed to tolerate being briefly unavailable. The `outbox_worker`'s own producer retries/reconnects independently and is monitored via `outbox_publish_lag_seconds` (§21 in `HLD.md`), not via the readiness probe.

---

## 5. API Specification & Payload Definitions

### 5.1 `POST /files/upload/initiate`

Request:

```json
{
  "conversation_id": "conv_123",
  "filename": "physics.pdf",
  "content_type": "application/pdf",
  "size_bytes": 5242880
}
```

Validation: `content_type` ∈ allowlist (`app/core/constants.py:ALLOWED_MIME_TYPES`); `0 < size_bytes <= settings.MAX_UPLOAD_SIZE_BYTES`; `filename` sanitized (no path separators).

Response `201 Created`:

```json
{
  "document_id": "018f2f3a-...-uuid7",
  "upload_url": "https://minio.internal/graphgpt-files-quarantine/...&X-Amz-Signature=...",
  "upload_method": "PUT",
  "form_data": null,
  "expires_in": 900
}
```

Errors: `400` invalid content-type/size, `401` unauthenticated, `429` upload rate limit exceeded.

### 5.2 `POST /files/{document_id}/complete`

Request: empty body (or optional `{"client_checksum": "..."}` for extra verification).

Response `200 OK`:

```json
{
  "document_id": "018f2f3a-...",
  "status": "UPLOADED",
  "etag": "d41d8cd98f00b204e9800998ecf8427e",
  "checksum_sha256": null,
  "size_bytes": 5242880,
  "version": 1
}
```

Errors: `403` not owner, `404` unknown document_id, `409` already completed with different terms, `410` presign expired, `422` verification failed (size/content-type mismatch).

### 5.3 `GET /files/{document_id}`

Response `200 OK`:

```json
{
  "document_id": "018f2f3a-...",
  "conversation_id": "conv_123",
  "owner_id": "user_456",
  "filename": "physics.pdf",
  "content_type": "application/pdf",
  "size_bytes": 5242880,
  "status": "READY",
  "current_version": 1,
  "created_at": "2026-09-06T15:00:00Z",
  "updated_at": "2026-09-06T15:02:00Z"
}
```

Errors: `403` no VIEWER access, `404` not found or soft-deleted.

### 5.4 `GET /files` (list, paginated)

Query params: `conversation_id?`, `status?`, `cursor?` (UUIDv7 keyset), `limit` (default 20, max 100).

Response `200 OK`:

```json
{
  "items": [ { "document_id": "...", "filename": "...", "status": "READY" } ],
  "next_cursor": "018f2f3b-..."
}
```

### 5.5 `GET /files/{document_id}/download`

Response `200 OK`:

```json
{
  "download_url": "https://minio.internal/graphgpt-files/...&X-Amz-Signature=...",
  "expires_in": 300
}
```

Errors: `403` no VIEWER access, `404` not found, `409` file not yet READY (optional policy — some deployments allow downloading `UPLOADED`/`PROCESSING` originals).

### 5.6 `GET /files/{document_id}/status`

```json
{ "document_id": "018f2f3a-...", "status": "PROCESSING", "retry_count": 0 }
```

### 5.7 `POST /files/{document_id}/retry`

Response `202 Accepted`:

```json
{ "document_id": "018f2f3a-...", "status": "PROCESSING", "retry_count": 1 }
```

Errors: `409` current status is not `FAILED`.

### 5.8 `DELETE /files/{document_id}`

Response `202 Accepted`:

```json
{ "document_id": "018f2f3a-...", "status": "DELETING" }
```

Errors: `403` requires `OWNER`, `409` retention lock active in MinIO (see [HLD §15](./HLD.md#15-delete-flow)).

### 5.9 `PUT /files/{document_id}/permissions`

Request:

```json
{ "principal_id": "user_789", "principal_type": "USER", "access_level": "VIEWER" }
```

Response `200 OK`: echoes the created/updated grant. Errors: `403` requires `OWNER`.

---

## 6. Sequence Flows

### 6.1 Initiate + Complete Upload (happy path, `AV_SCAN_ENABLED=false` — dev/local)

This diagram shows the simplest terminal case, where `complete_upload` itself emits `file.uploaded`. **In production, `AV_SCAN_ENABLED=true`** and this same `/complete` call instead lands the file in `UPLOADED` inside the quarantine bucket only — `file.uploaded` is deferred until the quarantine flow in §6.6 promotes the object, per the single-authoritative-flow described in §4.5.

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as API Router
    participant US as UploadService
    participant DB as PostgreSQL
    participant MinIO
    participant OB as outbox_events

    Client->>API: POST /files/upload/initiate
    API->>US: initiate_upload(...)
    US->>US: validate content_type & size
    US->>US: generate document_id (UUIDv7)
    US->>DB: INSERT files (UPLOADING), INSERT upload_jobs (PENDING)
    US->>MinIO: PresignedPutObject(bucket, key, 15m)
    MinIO-->>US: presigned URL
    US-->>API: document_id, upload_url
    API-->>Client: 201 Created

    Client->>MinIO: PUT bytes (direct, presigned URL)
    MinIO-->>Client: 200 OK (ETag)

    Client->>API: POST /files/{document_id}/complete
    API->>US: complete_upload(document_id)
    US->>DB: SELECT files, upload_jobs FOR UPDATE
    US->>MinIO: StatObject(bucket, key)
    MinIO-->>US: size, content_type, ETag
    US->>US: compare against upload_jobs.expected_*
    US->>DB: BEGIN
    US->>DB: UPDATE files SET status=UPLOADED, etag=ETag
    US->>DB: INSERT file_versions (v1)
    US->>OB: INSERT outbox_events (file.uploaded)  %% dev-only path, AV_SCAN_ENABLED=false — see §6.6 for production
    US->>DB: COMMIT
    US-->>API: FileResponse(UPLOADED)
    API-->>Client: 200 OK
```

### 6.2 Complete Upload — Verification Failure

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as API Router
    participant US as UploadService
    participant MinIO
    participant DB as PostgreSQL

    Client->>API: POST /files/{document_id}/complete
    API->>US: complete_upload(document_id)
    US->>MinIO: StatObject(bucket, key)
    alt Object missing
        MinIO-->>US: NoSuchKey
        US-->>API: raise ObjectNotFoundError
        API-->>Client: 422 (object not found — upload never reached MinIO)
    else Size mismatch
        MinIO-->>US: stat.size != expected_size
        US->>DB: UPDATE files SET status=FAILED
        US-->>API: raise SizeMismatchError
        API-->>Client: 422 (size mismatch)
    end
```

### 6.3 Outbox Publishing

```mermaid
sequenceDiagram
    autonumber
    participant OW as Outbox Worker
    participant DB as PostgreSQL
    participant Kafka

    loop every poll_interval
        OW->>DB: SELECT * FROM outbox_events WHERE processed=FALSE ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED
        DB-->>OW: rows
        loop each row
            OW->>Kafka: publish(topic=event_type, payload)
            Kafka-->>OW: ACK
            OW->>DB: UPDATE outbox_events SET processed=TRUE WHERE id=row.id
        end
    end
```

### 6.4 Download

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as API Router
    participant PS as PermissionService
    participant DS as DownloadService
    participant MinIO

    Client->>API: GET /files/{document_id}/download
    API->>PS: assert_access(document_id, requester_id, VIEWER)
    PS-->>API: OK (owner or granted VIEWER+)
    API->>DS: get_download_url(document_id)
    DS->>MinIO: PresignedGetObject(bucket, key, 5m, response-content-disposition)
    MinIO-->>DS: signed URL
    DS-->>API: download_url
    API-->>Client: 200 OK {download_url}
    Client->>MinIO: GET (direct, presigned URL)
```

### 6.5 Delete

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as API Router
    participant PS as PermissionService
    participant FS as FileService
    participant MinIO
    participant DB as PostgreSQL
    participant OB as outbox_events

    Client->>API: DELETE /files/{document_id}
    API->>PS: assert_access(document_id, requester_id, OWNER)
    API->>FS: delete_file(document_id)
    FS->>DB: UPDATE files SET status=DELETING
    FS->>MinIO: RemoveObject(bucket, key) [all versions]
    alt Retention lock active
        MinIO-->>FS: AccessDenied (object locked)
        FS->>DB: UPDATE files SET status=FAILED
        FS-->>API: raise RetentionLockedError
        API-->>Client: 409 Conflict
    else Success
        MinIO-->>FS: 204
        FS->>DB: UPDATE files SET status=DELETED, deleted_at=now()
        FS->>OB: INSERT outbox_events (file.deleted)
        FS-->>API: OK
        API-->>Client: 202 Accepted
    end
```

### 6.6 Quarantine / Malware Scan

```mermaid
sequenceDiagram
    autonumber
    participant MinIO as MinIO (quarantine bucket)
    participant Kafka
    participant AV as AV Scan Consumer
    participant Files as MinIO (files bucket)
    participant DB as PostgreSQL
    participant OB as outbox_events

    MinIO->>Kafka: s3:ObjectCreated (quarantine bucket)
    Kafka->>AV: consume event (dedupe on event_id, §3.7)
    AV->>DB: UPDATE files SET status=SCANNING (from UPLOADED)
    AV->>MinIO: GetObject / stream scan + magic-byte check + SHA-256 hash
    alt Clean
        AV->>Files: CopyObject(quarantine -> files, same key)
        AV->>DB: UPDATE files SET storage_bucket='graphgpt-files', checksum_sha256=<computed>
        AV->>MinIO: RemoveObject(quarantine)
        AV->>DB: UPDATE files SET status=UPLOADED (from SCANNING)
        AV->>OB: INSERT outbox_events (file.uploaded)  %% only place this event is written when AV_SCAN_ENABLED=true
        Note over OB: Retrieval Service consumes file.uploaded and emits file.processing.started,<br/>which is what actually moves File Service to PROCESSING (§3.6)
    else Infected
        AV->>MinIO: RemoveObject(quarantine)
        AV->>DB: UPDATE files SET status=FAILED, last_error_code='AV_SCAN_INFECTED'
        AV->>OB: INSERT outbox_events (file.scan_failed)
        Note over OB: file.uploaded is never emitted for this document_id
    end
```

### 6.7 Processing Status Contract (Retrieval Service → File Service)

```mermaid
sequenceDiagram
    autonumber
    participant OB as outbox_events
    participant Kafka
    participant RS as Retrieval Service
    participant PSC as processing_status_consumer.py
    participant DB as PostgreSQL

    OB->>Kafka: file.uploaded
    Kafka->>RS: consume (dedupe on event_id)
    RS->>Kafka: publish file.processing.started
    Kafka->>PSC: consume (dedupe on event_id)
    PSC->>DB: transition UPLOADED/PROCESSING -> PROCESSING (no-op if already PROCESSING)

    RS->>RS: parse -> chunk -> embed -> write to Milvus

    alt Success
        RS->>Kafka: publish file.processing.completed {chunk_count}
        Kafka->>PSC: consume (dedupe on event_id)
        PSC->>DB: transition PROCESSING -> READY
    else Failure
        RS->>Kafka: publish file.processing.failed {error_code, error_message}
        Kafka->>PSC: consume (dedupe on event_id)
        PSC->>DB: transition PROCESSING -> FAILED, set last_error_code/last_error_message
    end
```

Note that File Service never polls or calls Retrieval Service — the entire contract is Kafka events in both directions, keeping the two services' failure domains and databases fully decoupled.

### 6.8 Reconciliation Sweep

```mermaid
sequenceDiagram
    autonumber
    participant RW as Reconciliation Worker
    participant DB as PostgreSQL
    participant Kafka as minio.object.events

    loop every RECONCILIATION_INTERVAL_SECONDS
        RW->>DB: UPDATE upload_jobs SET status='EXPIRED' WHERE status='PENDING' AND expires_at < now()
        RW->>DB: SELECT files WHERE status='UPLOADING' AND created_at < now() - 1h
        RW->>DB: transition abandoned rows -> FAILED
        RW->>Kafka: consume backlog of ObjectCreated events
        RW->>DB: cross-check each object key against files table
        alt No matching row found
            RW->>RW: emit metric reconciliation_orphans_total, log for manual review
        end
    end
```

---

## 7. Configuration (`app/core/config.py`)

| Variable | Example | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://user:pass@postgres:5432/file_service` | PostgreSQL connection |
| `MINIO_ENDPOINT` | `minio.internal:9000` | MinIO S3-API endpoint (no scheme) |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | — | Scoped IAM credentials (never root) |
| `MINIO_USE_TLS` | `true` | Enforce HTTPS to MinIO in prod |
| `BUCKET_FILES` | `graphgpt-files` | Primary bucket |
| `BUCKET_QUARANTINE` | `graphgpt-files-quarantine` | Pre-scan staging bucket |
| `MINIO_KAFKA_NOTIFICATION_ARN` | `arn:minio:sqs::file-events:kafka` | Target for MinIO bucket notifications |
| `KAFKA_BROKERS` | `kafka:9092` | aiokafka bootstrap servers |
| `UPLOAD_PRESIGN_TTL_SECONDS` | `900` | Upload URL expiry |
| `DOWNLOAD_PRESIGN_TTL_SECONDS` | `300` | Download URL expiry |
| `MAX_UPLOAD_SIZE_BYTES` | `104857600` (100MB) | Global upload cap |
| `ALLOWED_MIME_TYPES` | `application/pdf,text/plain,...` | Content-type allowlist |
| `AV_SCAN_ENABLED` | `true` | Toggles the quarantine flow (§4.5) |
| `UPLOAD_STRICT_POLICY` | `true` | Use presigned POST policy (enforced at MinIO) vs. bare PUT |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | Outbox worker cadence |
| `RECONCILIATION_INTERVAL_SECONDS` | `300` | Reconciliation worker cadence |

---

## 8. Error Handling & Exception Mapping (`app/core/exceptions.py`)

| Exception | HTTP Status | Notes |
|---|---|---|
| `InvalidContentTypeError` | 400 | Raised by `validation/content_type.py` |
| `UploadSizeExceededError` | 400 | Raised by `validation/size_limits.py` |
| `PermissionDeniedError` | 403 | Raised by `permission_service.assert_access` |
| `FileNotFoundError` | 404 | Unknown/soft-deleted `document_id` |
| `AlreadyCompletedError` | 409 | `/complete` called with mismatched terms after already UPLOADED |
| `InvalidStateTransitionError` | 409 | Lifecycle guard rejection (e.g. retry on non-FAILED) |
| `RetentionLockedError` | 409 | MinIO object lock prevents delete |
| `PresignExpiredError` | 410 | `upload_jobs.expires_at` passed |
| `ObjectNotFoundError` | 422 | `StatObject` found nothing at `/complete` time |
| `SizeMismatchError` / `ContentTypeMismatchError` | 422 | Verification failed against `upload_jobs` expectations |
| `UploadRateLimitExceededError` | 429 | Per-user upload throttle |
| `MinioUnavailableError` | 503 | MinIO connectivity failure — surfaced, not swallowed |

All handlers registered centrally in `main.py` via `app.add_exception_handler(...)`, returning a consistent `ErrorResponse` schema (`{"error_code": ..., "message": ..., "request_id": ...}`).

---

## 9. Security Hardening Policies

1. **Scoped MinIO credentials**: the File Service's access key is limited to `PutObject/GetObject/DeleteObject/ListBucket` on `graphgpt-files*` only (see [HLD §14](./HLD.md#14-security) IAM policy) — never the MinIO root user.
2. **No public bucket access**: `bucket_bootstrap.py` applies a deny-all-anonymous policy on every boot, self-healing against manual Console misconfiguration.
3. **Short-lived presigned URLs**: 15 minutes for uploads, 5 minutes for downloads — configurable but capped low by policy.
4. **Server-side trust boundary**: `object_verifier.py` is the only code path allowed to mark a file `UPLOADED`; client-declared size/content-type are never trusted past that point.
5. **Quarantine-first uploads**: when `AV_SCAN_ENABLED=true`, no object is copied into the publicly-referenceable `graphgpt-files` bucket until a scan consumer promotes it.
6. **Encryption at rest**: SSE-S3 enabled bucket-wide via `bucket_bootstrap.py`.
7. **Outbox isolation**: Kafka downtime never blocks the upload API — events queue durably in `outbox_events` (same guarantee as `auth-service`).
8. **Idempotent `/complete`**: prevents duplicate `file.uploaded` events and duplicate `file_versions` rows from retried client calls.
9. **Row-level authorization**: every read/write route resolves through `permission_service.assert_access`, never trusting `owner_id`/`document_id` supplied without a DB-backed ownership/grant check.

---

## 10. Testing Strategy

* **Unit** (`tests/unit/`): `key_builder`, `object_verifier` (mocked `Minio` client), `lifecycle_service` transition table, `permission_service` rank comparison — no network/DB required.
* **Integration** (`tests/integration/`): spun up against real Postgres + MinIO test containers (`testcontainers-python`) to validate the full initiate → PUT → complete → download → delete round trip, including outbox → Kafka publishing via an embedded/test broker.
* **Contract**: `file.uploaded` / `file.deleted` / `file.processing_retry` payload schemas (produced by File Service) and `file.processing.started` / `file.processing.completed` / `file.processing.failed` payload schemas (consumed from Retrieval Service, §3.6) are snapshot-tested in both directions so a schema change on either side is caught before it breaks the other service — the current GraphGPT architecture has a single downstream consumer of `file.uploaded`, **Retrieval Service** (parse → chunk → embed → Milvus), not separate embedding/graph services.
* **Idempotency tests**: replaying the same `event_id` twice through `outbox_worker` and through `processing_status_consumer` must be asserted to produce no duplicate side effects (§3.7) — e.g. a duplicate `file.processing.completed` must not double-count `chunk_count` anywhere or error on the now-no-op `READY -> READY` transition.
