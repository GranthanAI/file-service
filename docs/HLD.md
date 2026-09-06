# File Service HLD (MinIO-based Implementation)

## 1. Overview

The **File Service** owns the complete lifecycle of files uploaded by users in GraphGPT: accepting uploads, storing raw bytes durably, tracking metadata/ownership/permissions, and notifying downstream services asynchronously when a file is ready for processing.

This document expands on [`HLD_demo.md`](./HLD_demo.md) with a concrete implementation using **MinIO** as the object-storage backend (instead of a generic "S3/MinIO" placeholder), including bucket layout, IAM policy, presigned-URL flow, event wiring, deployment topology, and operational concerns specific to running MinIO in production.

### Core responsibilities

* File upload initialization
* File validation (type, size, ownership)
* `document_id` generation
* Raw file storage in MinIO
* File metadata management in PostgreSQL
* Ownership and permission enforcement
* Upload completion verification
* File lifecycle / processing-status tracking
* Retry management
* Secure file download/access via presigned URLs
* File deletion (object + metadata)
* Publishing `file.uploaded` (and related) events via Kafka

### Non-goals

The File Service does **not** parse, chunk, embed, index, search, rerank, or generate LLM responses. It hands off to downstream services purely via the `file.uploaded` event.

---

## 2. Why MinIO

MinIO is an S3-API-compatible, self-hosted object store. It is chosen here because:

* **S3-API compatible** — the service can use the standard AWS SDK (`aws-sdk-go-v2`, `boto3`, `@aws-sdk/client-s3`, etc.) or the native `minio-go` / `minio-py` SDKs; if GraphGPT ever migrates to AWS S3, application code barely changes.
* **Self-hostable** — no cloud vendor lock-in; can run in the same Kubernetes cluster as the rest of GraphGPT, keeping latency low and avoiding egress costs.
* **Erasure coding** — built-in data redundancy (similar to RAID) without needing a separate replication layer.
* **Bucket notifications** — MinIO can publish object events (`s3:ObjectCreated:*`, `s3:ObjectRemoved:*`) directly to Kafka/webhooks, which is useful as a **defense-in-depth signal** alongside the File Service's own outbox-driven `file.uploaded` event (see [§12](#12-minio-event-notifications-defense-in-depth)).
* **Presigned URLs** — first-class support for presigned PUT/GET URLs, enabling direct client-to-storage upload/download without proxying bytes through the File Service.
* **Object locking & versioning** — native support for immutability and version history, which maps directly to the `file_versions` requirement.

---

## 3. High-Level Architecture

```text
                         Frontend / BFF
                               │
                               ▼
                         File Service  (stateless pods)
                    ┌──────────┼──────────┐
                    │          │          │
                    ▼          ▼          ▼
              PostgreSQL      MinIO       Kafka
               Metadata     Raw Files   file.uploaded
```

```text
Client
  │
  ├──(1) initiate upload──▶ File Service ──▶ PostgreSQL (create metadata row, PENDING)
  │                              │
  │◀────(2) presigned PUT URL───┘
  │
  ├──(3) PUT file bytes─────────────────────▶ MinIO
  │
  ├──(4) complete upload───▶ File Service ──▶ MinIO (HEAD/stat check)
  │                              │
  │                              ├──▶ PostgreSQL (UPLOADED + outbox_events row, same txn)
  │                              │
  │◀────(5) 200 OK ──────────────┘
  │
                          Outbox Worker ──▶ Kafka (file.uploaded)
```

Direct-to-MinIO upload/download keeps large binary transfer off the File Service's request path entirely — the service only ever handles small JSON payloads and short-lived credential issuance.

---

## 4. File Service Components

```text
File Service
│
├── Upload Controller          (initiate / complete handlers)
├── File Validation            (MIME sniffing, size limits, extension allowlist)
├── File Metadata Manager      (PostgreSQL CRUD)
├── MinIO Storage Manager      (bucket/key naming, presigned URL issuance, stat/verify, delete)
├── Permission Manager         (ownership + ACL checks)
├── Lifecycle Manager          (status state machine)
├── Processing Status Manager  (status API)
├── Retry Manager
├── Outbox Worker              (polls outbox_events → publishes to Kafka)
└── MinIO Notification Consumer (optional reconciliation signal, see §12)
```

The service is **stateless**; all durable state lives in PostgreSQL (metadata) and MinIO (bytes). Any pod can serve any request.

---

## 5. MinIO Deployment Topology

### 5.1 Cluster mode (production)

Run MinIO in **distributed erasure-coded mode** for durability and availability — minimum 4 nodes/drives to tolerate node loss:

```text
              ┌───────────────── MinIO Load Balancer / Ingress ─────────────────┐
              │                                                                  │
         ┌────▼────┐        ┌─────────┐        ┌─────────┐        ┌─────────┐
         │ minio-0 │◀──────▶│ minio-1 │◀──────▶│ minio-2 │◀──────▶│ minio-3 │
         │ (drive) │        │ (drive) │        │ (drive) │        │ (drive) │
         └─────────┘        └─────────┘        └─────────┘        └─────────┘
```

* Deploy via the **MinIO Kubernetes Operator** (`Tenant` CRD) with 4+ pods and dedicated PersistentVolumes (one per drive, not shared).
* Erasure coding: with 4 drives, MinIO tolerates loss of up to 2 drives (`EC:2` parity by default) while remaining read/write available.
* Front the tenant with a `Service` + Ingress/Load Balancer for S3-API traffic (port 9000) and a separate one for the MinIO Console (port 9001), console access restricted to internal admin network only.
* TLS termination at the MinIO tenant itself (or at the ingress) — never serve the S3 API over plaintext in production.

### 5.2 Non-production (dev/staging)

Single-node MinIO (Docker Compose) is sufficient:

```yaml
# docker-compose.yml (excerpt)
services:
  minio:
    image: minio/minio:RELEASE.2024-01-01T00-00-00Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # Console
    volumes:
      - minio-data:/data
```

---

## 6. Bucket & Key Design

### 6.1 Buckets

Use a small, purpose-scoped set of buckets rather than one bucket per user/tenant (bucket sprawl is an operational anti-pattern in MinIO):

```text
graphgpt-files          # primary bucket for original uploaded files
graphgpt-files-quarantine   # files pending malware/AV scan (see §14)
```

### 6.2 Object key layout

```text
graphgpt-files/
  {owner_id}/
    {document_id}/
      v{version}/
        original.{ext}
```

Example:

```text
graphgpt-files/user_456/doc_789/v1/original.pdf
graphgpt-files/user_456/doc_789/v2/original.pdf
```

* `owner_id` prefix enables coarse-grained per-user listing/quota auditing and simplifies bulk deletion on account closure.
* `document_id` is the File Service–generated identifier (see [§8](#8-document-id)); it is the join key against PostgreSQL.
* Versioning is expressed both as a **key prefix** (`v{n}`) and via **MinIO's native object versioning** enabled on the bucket — the prefix keeps versions independently addressable/listable, native versioning protects against accidental overwrite/delete of the same key.

### 6.3 Bucket configuration

| Setting | Value | Purpose |
|---|---|---|
| Versioning | Enabled | Protects against accidental overwrite/delete; backs `file_versions` |
| Object Locking | Enabled (governance mode, optional retention) | Prevents deletion during legal hold / compliance windows |
| Server-Side Encryption | SSE-S3 (`AES256`) via MinIO KMS, or SSE-KMS with an external KMS (Vault/HashiCorp) | Encryption at rest |
| Lifecycle rule | Expire objects in `graphgpt-files-quarantine` after 24h if not promoted | Prevents orphaned quarantined files from accumulating |
| Lifecycle rule | Transition old versions to a cheaper storage class / expire after N days (policy-driven) | Cost control for `file_versions` |
| Public access | **Blocked** — bucket policy denies anonymous/public reads entirely | All access via presigned URLs or the File Service |

---

## 7. Storage Design — PostgreSQL

### `files`

```text
document_id      UUID PRIMARY KEY
conversation_id  UUID
owner_id         UUID
filename         TEXT
content_type     TEXT
size_bytes       BIGINT
storage_bucket   TEXT        -- e.g. "graphgpt-files"
storage_key      TEXT        -- e.g. "user_456/doc_789/v1/original.pdf"
checksum         TEXT        -- ETag / SHA-256, verified against MinIO
status           TEXT        -- UPLOADING | UPLOADED | PROCESSING | READY | FAILED | DELETING | DELETED
version          INT
created_at       TIMESTAMPTZ
updated_at       TIMESTAMPTZ
deleted_at       TIMESTAMPTZ
```

### Supporting tables

```text
upload_jobs        -- tracks in-flight presigned-upload sessions (document_id, expires_at, status)
file_versions       -- document_id, version, storage_key, checksum, size_bytes, created_at
file_permissions    -- document_id, principal_id, principal_type (user/team), access_level
outbox_events       -- event_id, aggregate_id, event_type, payload, published_at (nullable)
```

`storage_bucket` + `storage_key` together form the pointer into MinIO; PostgreSQL never stores file bytes.

---

## 8. Document ID

The File Service generates and owns `document_id` (UUIDv7 recommended for time-sortability):

```text
Upload Request
      ↓
File Service
      ↓
Generate document_id (UUIDv7)
      ↓
doc_01hz8...
```

Other services reference this ID; the File Service remains the source of truth for its metadata and storage location.

---

## 9. Upload Design (Presigned PUT via MinIO)

### Step 1 — Initiate

```http
POST /files/upload/initiate
```

Request:

```json
{
  "conversation_id": "conv_123",
  "filename": "physics.pdf",
  "content_type": "application/pdf",
  "size_bytes": 5242880
}
```

File Service flow:

```text
Validate request (content_type allowlist, size_bytes <= MAX_UPLOAD_SIZE)
   ↓
Generate document_id (UUIDv7)
   ↓
Insert `files` row (status=UPLOADING) + `upload_jobs` row
   ↓
Build storage_key = "{owner_id}/{document_id}/v1/original.{ext}"
   ↓
Call MinIO PresignedPutObject(bucket, key, expiry=15m, content-type, content-length-range)
   ↓
Return presigned URL
```

Using the MinIO Go SDK, this is:

```go
presignedURL, err := minioClient.PresignedPutObject(
    ctx,
    "graphgpt-files",
    storageKey,
    15*time.Minute,
)
```

To enforce content-type and max size at the storage layer (not just app-layer), use a **presigned POST policy** instead of a bare presigned PUT when stricter guarantees are needed:

```go
policy := minio.NewPostPolicy()
policy.SetBucket("graphgpt-files")
policy.SetKey(storageKey)
policy.SetContentType(contentType)
policy.SetContentLengthRange(1, maxUploadSizeBytes)
policy.SetExpires(time.Now().UTC().Add(15 * time.Minute))
url, formData, err := minioClient.PresignedPostPolicy(ctx, policy)
```

Response:

```json
{
  "document_id": "doc_01hz8",
  "upload_url": "https://minio.internal/graphgpt-files/...&X-Amz-Signature=...",
  "expires_in": 900
}
```

### Step 2 — Direct client → MinIO upload

```text
Frontend
    │  PUT (or POST multipart) with presigned URL
    ▼
MinIO
```

The File Service is not on the data path for the binary transfer, so throughput/memory on File Service pods is unaffected by file size.

### Step 3 — Complete

```http
POST /files/{document_id}/complete
```

File Service verifies against MinIO directly (never trusts the client's word that upload succeeded):

```text
StatObject(bucket, storage_key)
   ↓
Compare: object exists? size_bytes matches? content_type matches? checksum (ETag) matches?
   ↓
Check requester == owner_id (authorization)
   ↓
UPLOADING → UPLOADED   (DB transaction)
   +
Insert outbox_events row: file.uploaded   (same transaction)
```

```go
info, err := minioClient.StatObject(ctx, bucket, storageKey, minio.StatObjectOptions{})
if err != nil {
    return ErrUploadNotFound
}
if info.Size != expectedSize {
    return ErrSizeMismatch
}
```

If verification fails, the upload_job and file row transition to `FAILED` rather than silently completing — the client can re-initiate.

---

## 10. Download Design (Presigned GET via MinIO)

```http
GET /files/{document_id}/download
```

```text
Client
   ↓
File Service
   ↓
Authenticate + Authorization (file_permissions check)
   ↓
MinIO PresignedGetObject(bucket, key, expiry=5m)
   ↓
302 redirect (or JSON with signed URL) to MinIO
   ↓
Client downloads directly from MinIO
```

```go
url, err := minioClient.PresignedGetObject(
    ctx, "graphgpt-files", storageKey, 5*time.Minute, nil,
)
```

Short expiry (minutes, not hours) limits the exposure window of a leaked URL. Response-content-disposition can be set on the presigned URL to force a friendly download filename regardless of the internal storage key.

---

## 11. File Lifecycle & Processing Status

Unchanged from the base design — MinIO does not participate in the state machine itself, it only holds bytes for states `UPLOADED` through `DELETED`:

```text
UPLOADING → UPLOADED → PROCESSING → READY
                 │
                 ▼
              FAILED ──(retry)──▶ PROCESSING

READY → DELETING → DELETED
```

```http
GET /files/{document_id}/status
```

```json
{ "document_id": "doc_01hz8", "status": "PROCESSING" }
```

---

## 12. MinIO Event Notifications (defense-in-depth)

In addition to the File Service's own outbox-driven `file.uploaded` Kafka event (the **primary, authoritative** signal — see [§13](#13-reliable-event-publishing-transactional-outbox)), MinIO can be configured to publish **bucket notifications** directly to Kafka on `s3:ObjectCreated:Put`:

```bash
mc admin config set myminio notify_kafka:file-events \
  brokers="kafka:9092" topic="minio.object.events"

mc event add myminio/graphgpt-files arn:minio:sqs::file-events:kafka \
  --event put
```

**Why keep both:**

* The File Service's outbox event is the one downstream services subscribe to for triggering document processing — it carries full metadata (`owner_id`, `conversation_id`, etc.) that a raw MinIO event does not have.
* The raw MinIO event is consumed only by an internal **reconciliation job** that periodically cross-checks "objects that exist in MinIO but have no corresponding `UPLOADED` row / outbox event within N minutes" — catching the edge case where a client uploaded bytes but never called `/complete`. This is an operational safety net, not part of the main event contract.

```text
MinIO ObjectCreated ──▶ Kafka (minio.object.events) ──▶ Reconciliation Job
                                                              │
                                                              ▼
                                              Alert / auto-expire orphaned upload_jobs
```

---

## 13. Reliable Event Publishing (Transactional Outbox)

```text
                PostgreSQL
        ┌──────────────────────┐
        │ files                │
        │ outbox_events        │
        └──────────┬───────────┘
                   │  (single ACID transaction on complete-upload)
                   ▼
              Outbox Worker (polls / CDC)
                   │
                   ▼
                 Kafka  → file.uploaded
```

`file.uploaded` payload:

```json
{
  "event_id": "evt_001",
  "event_type": "file.uploaded",
  "version": 1,
  "document_id": "doc_01hz8",
  "conversation_id": "conv_123",
  "owner_id": "user_456",
  "filename": "physics.pdf",
  "content_type": "application/pdf",
  "storage": {
    "provider": "minio",
    "bucket": "graphgpt-files",
    "key": "user_456/doc_01hz8/v1/original.pdf",
    "etag": "d41d8cd98f00b204e9800998ecf8427e"
  },
  "timestamp": "2026-09-06T15:00:00Z"
}
```

The event carries a **storage reference**, not bytes. Consumers use their own MinIO client (read-only credentials scoped to `graphgpt-files`, GetObject only) to fetch the file when they need to process it, rather than receiving a presigned URL in the event (presigned URLs expire; a durable storage reference does not).

---

## 14. Security

| Concern | MinIO-specific control |
|---|---|
| Credentials | File Service holds a scoped **IAM policy** access key (see below) — never the MinIO root credentials |
| Public access | Bucket policy explicitly denies anonymous access; `mc anonymous set none` on all buckets |
| Presigned URL abuse | Short expiry (15m upload / 5m download); one-time use enforced at the app layer via `upload_jobs` state (a completed job's URL is meaningless after `/complete`) |
| Encryption at rest | SSE-S3 or SSE-KMS enabled bucket-wide |
| Encryption in transit | TLS on the MinIO S3 API endpoint (9000) and Console (9001); reject plaintext |
| Malware scanning | Uploads land first in `graphgpt-files-quarantine`; an AV scan (e.g. ClamAV sidecar) triggered off the MinIO `ObjectCreated` event promotes clean files to `graphgpt-files` via server-side `CopyObject` and deletes the quarantined copy — infected files are deleted and the `files` row is marked `FAILED`. This is the **only** promotion path: `file.uploaded` is emitted exclusively on successful promotion out of quarantine, never at initial upload — see the LLD's single authoritative upload/quarantine flow for the full state sequence. |
| File-type validation | Content-type allowlist enforced at presigned-POST-policy level (declared MIME, checked exactly — not by prefix match) plus magic-byte sniffing performed during the AV-scan pass, since that is the first point the object's actual bytes are read server-side (`/complete` itself only calls `StatObject`, which inspects metadata, not content) |
| Object immutability | Object Locking (governance mode) for files under legal hold |
| Least privilege | Downstream processing services get a **separate, read-only** MinIO access key scoped to `GetObject` on `graphgpt-files/*` only — never write/delete |

### IAM policy example (File Service's key)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:GetObjectVersion"],
      "Resource": ["arn:aws:s3:::graphgpt-files/*", "arn:aws:s3:::graphgpt-files-quarantine/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::graphgpt-files", "arn:aws:s3:::graphgpt-files-quarantine"]
    }
  ]
}
```

Applied via `mc admin policy` and attached to a dedicated MinIO service account — rotated periodically, never shared with other services.

---

## 15. Delete Flow

```http
DELETE /files/{document_id}
```

```text
Permission check
     ↓
files.status → DELETING
     ↓
MinIO RemoveObject(bucket, key)  -- all versions if object versioning is on, or
MinIO RemoveObject with versionId for a specific version
     ↓
PostgreSQL: files.status → DELETED, deleted_at = now()
     ↓
Insert outbox_events row: file.deleted
     ↓
Downstream cleans up indexed representations (vector store, chunks, etc.) on file.deleted
```

If Object Locking / retention is active on a version, `RemoveObject` will fail until the retention period lapses — the File Service surfaces this as a `409 Conflict` rather than silently no-oping.

---

## 16. File Versioning

```text
document_id = doc_01hz8
  v1 → user_456/doc_01hz8/v1/original.pdf
  v2 → user_456/doc_01hz8/v2/original_updated.pdf
```

Each new version is a **new object key** (not an in-place overwrite), tracked in `file_versions`. This works alongside — not instead of — MinIO's native bucket versioning: the key-prefix versioning gives predictable, independently-listable version objects; native MinIO versioning is a safety net against accidental delete/overwrite of any single key.

---

## 17. Idempotency

* `POST /files/{document_id}/complete` is idempotent on `document_id`: if already `UPLOADED`/`READY`, return the existing state rather than re-verifying/re-publishing.
* Outbox events are keyed by `event_id` (UUID); consumers dedupe on `event_id`.
* `PresignedPutObject` uploads are idempotent at the storage layer — re-uploading the same key simply overwrites the (still-unverified) object; verification only happens once, at `/complete`.

---

## 18. API Design

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/files/upload/initiate` | Create metadata row, return MinIO presigned PUT/POST |
| `POST` | `/files/{document_id}/complete` | Verify object in MinIO, mark UPLOADED, enqueue outbox event |
| `GET` | `/files/{document_id}` | Get metadata |
| `GET` | `/files` | List files (paginated, filterable by conversation/owner) |
| `GET` | `/files/{document_id}/download` | Return MinIO presigned GET |
| `GET` | `/files/{document_id}/status` | Get processing status |
| `POST` | `/files/{document_id}/retry` | Reset FAILED → PROCESSING, republish event |
| `DELETE` | `/files/{document_id}` | Remove from MinIO + mark DELETED |

---

## 19. Scalability

```text
                    Load Balancer
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          File Pod    File Pod    File Pod   (stateless, HPA on CPU/RPS)
              │          │          │
              └──────────┼──────────┘
                         │
                ┌────────┴────────┐
                ▼                 ▼
           PostgreSQL      MinIO Tenant (4+ node erasure-coded cluster)
                         │
                         ▼
                       Kafka
```

* File Service pods scale horizontally behind a load balancer/HPA — no local state.
* MinIO scales by adding server pools to the tenant (`Tenant.spec.pools`), rebalancing data across the expanded pool.
* Direct-to-MinIO upload/download means File Service compute scales with **request rate**, not with **bytes transferred**.

---

## 20. Failure Handling

| Failure | Handling |
|---|---|
| MinIO node/drive down | Erasure coding tolerates up to parity-count drive/node loss; reads/writes continue |
| Presigned URL expired before upload | Client re-initiates `/upload/initiate` |
| `/complete` called before object exists in MinIO | `StatObject` fails → return `409`, client retries after actual upload finishes |
| Duplicate `/complete` calls | Idempotent — no duplicate rows/events |
| PostgreSQL unavailable | Fail fast on initiate/complete; MinIO itself is untouched |
| Kafka unavailable | Outbox worker retries with backoff; event isn't lost since it's durably in `outbox_events` |
| Malware detected | File deleted from quarantine bucket, `files.status = FAILED`, notify owner |
| Retention lock blocks delete | Surface `409 Conflict`, do not silently skip |
| Service pod crash | Stateless — Kubernetes reschedules, no data loss (nothing was held in memory) |

---

## 21. Observability

### Metrics

```text
upload_requests_total
upload_failures_total{reason}
upload_duration_seconds
minio_stat_object_duration_seconds
minio_presign_duration_seconds
file_size_bytes (histogram)
processing_status_count{status}
download_requests_total
delete_requests_total
outbox_publish_lag_seconds
```

MinIO itself exposes Prometheus metrics natively (`/minio/v2/metrics/cluster`) — scrape for: disk usage per drive, erasure-set health, API request latency/errors, replication lag (if bucket replication is configured for DR).

### Logs

Every request/event traceable via `request_id`, `document_id`, `owner_id`, `event_id`, and the MinIO `x-amz-request-id` returned on each storage call (correlate File Service logs with MinIO server logs).

### Tracing

```text
BFF → File Service → PostgreSQL
                   → MinIO (StatObject / Presign / RemoveObject)
                   → Kafka (outbox publish)
```

---

## 22. Backup & Disaster Recovery

* **Bucket replication**: configure MinIO site replication (or `mc mirror`) from the primary tenant to a secondary cluster/region for DR.
* **PostgreSQL** and **MinIO** must be backed up in a way that keeps them consistent enough that `storage_key` references in PostgreSQL always resolve — prefer point-in-time recovery for PostgreSQL combined with MinIO versioning (never hard-delete objects immediately; use a soft-delete lifecycle rule with a grace period before permanent expiry).
* Regularly test restore: an object present in a DR MinIO cluster but whose PostgreSQL row was lost is orphaned data; the reverse (row without object) is a broken reference — the reconciliation job (§12) should catch both.

---

## 23. Final File Service Boundary

```text
                 FILE SERVICE
                      │
       ┌──────────────┼──────────────┐
       │              │              │
       ▼              ▼              ▼
    Upload         Metadata       Permissions
       │              │              │
       ▼              ▼              ▼
     MinIO        PostgreSQL      PostgreSQL
       │
       ▼
  Lifecycle / Status
       │
       ▼
 file.uploaded (outbox → Kafka)
```

**Key principle:** File Service manages the file, not the document intelligence. It owns Upload → Storage (MinIO) → Metadata → Permissions → Lifecycle → Event. After `file.uploaded`, downstream services take over; the File Service never parses, chunks, embeds, indexes, or generates.
