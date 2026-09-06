# File Service HLD

## 1. Overview

The **File Service** manages the complete lifecycle of files uploaded by users in GraphGPT.

Its responsibility is limited to **file management and storage**. It should not perform document parsing, chunking, embedding, vector search, or LLM generation.

### Core responsibilities

* File upload initialization
* File validation
* `document_id` generation
* Raw file storage in S3/MinIO
* File metadata management in PostgreSQL
* Ownership and permissions
* Upload completion
* File lifecycle/status management
* Processing status tracking
* Retry management
* File download/access
* File deletion
* Publishing `file.uploaded` event

---

# 2. Problem Statement

GraphGPT users need to upload documents that can later be used as knowledge during conversations.

The File Service must provide a reliable and scalable mechanism to:

* Accept files from users.
* Store the original files safely.
* Maintain metadata and ownership.
* Associate files with conversations.
* Track the state of each file.
* Notify downstream processing through an asynchronous event.
* Avoid making file uploads dependent on expensive document-processing operations.

The actual document processing is triggered asynchronously after the File Service publishes the `file.uploaded` event.

---

# 3. High-Level Architecture

```text
                         Frontend / BFF
                               │
                               ▼
                         File Service
                    ┌──────────┼──────────┐
                    │          │          │
                    ▼          ▼          ▼
              PostgreSQL    S3/MinIO    Kafka
               Metadata      Raw File   file.uploaded
```

The File Service therefore sits between the client and the file infrastructure:

```text
Client
  ↓
File Service
  ├── PostgreSQL
  ├── S3/MinIO
  └── Kafka
```

The source HLD defines File Service with **S3/MinIO + PostgreSQL** as primary storage and Kafka/Redis as supporting infrastructure. 

---

# 4. File Service Components

```text
File Service
│
├── Upload Controller
│
├── File Validation
│
├── File Metadata Manager
│
├── Storage Manager
│
├── Permission Manager
│
├── Lifecycle Manager
│
├── Processing Status Manager
│
├── Retry Manager
│
└── Kafka Event Publisher
```

The service should remain **stateless**, with persistent state stored in PostgreSQL and S3/MinIO.

---

# 5. Storage Design

## 5.1 S3 / MinIO

S3/MinIO stores the actual uploaded file.

Example:

```text
graphgpt-files/
    user_456/
        doc_789/
            v1/
                original.pdf
```

The File Service stores only the storage reference in PostgreSQL.

---

## 5.2 PostgreSQL

PostgreSQL stores file metadata.

### `files`

```text
document_id
conversation_id
owner_id
filename
content_type
size_bytes
storage_bucket
storage_key
checksum
status
version
created_at
updated_at
deleted_at
```

### Supporting tables

```text
upload_jobs
file_versions
file_permissions
```

The source architecture specifically identifies these metadata structures for file management. 

---

# 6. Document ID

The **File Service generates `document_id`**.

Example:

```text
Upload Request
      ↓
File Service
      ↓
Generate document_id
      ↓
doc_789
```

The File Service becomes the owner of this identifier.

The ID can then be referenced by other services when associating a document with a conversation.

The File Service itself remains responsible for the document metadata and storage associated with that ID.

---

# 7. Upload Design

Use a two-step upload process.

### Step 1: Initiate

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

File Service:

```text
Validate
   ↓
Generate document_id
   ↓
Create metadata
   ↓
Generate storage key
   ↓
Generate presigned URL
```

Response:

```json
{
  "document_id": "doc_789",
  "upload_url": "presigned-url"
}
```

---

# 8. Direct Object Storage Upload

The client uploads the actual binary directly to S3/MinIO using the presigned URL.

```text
Frontend
    │
    │ file
    ▼
S3 / MinIO
```

The File Service does not need to proxy the complete file.

This reduces application-server bandwidth and allows large files to be uploaded efficiently.

---

# 9. Upload Completion

After the object has been uploaded:

```http
POST /files/{document_id}/complete
```

File Service verifies:

* Object exists
* File size
* Content type
* Checksum, if applicable
* User authorization

Then updates:

```text
UPLOADING
    ↓
UPLOADED
```

After successful completion, the File Service publishes:

```text
file.uploaded
```

---

# 10. Kafka Event

The File Service publishes a `file.uploaded` event containing metadata and a reference to the stored object.

Example:

```json
{
  "event_id": "evt_001",
  "event_type": "file.uploaded",
  "version": 1,
  "document_id": "doc_789",
  "conversation_id": "conv_123",
  "owner_id": "user_456",
  "filename": "physics.pdf",
  "content_type": "application/pdf",
  "storage": {
    "bucket": "graphgpt-files",
    "key": "user_456/doc_789/v1/original.pdf"
  },
  "timestamp": "2026-09-06T15:00:00Z"
}
```

The event should contain a **storage reference, not the file binary**.

The source HLD defines `file.uploaded` as a File Service-produced Kafka event. 

---

# 11. File Lifecycle

The File Service maintains the file's lifecycle.

```text
UPLOADING
     │
     ▼
UPLOADED
     │
     ▼
PROCESSING
     │
     ▼
READY
```

Failure:

```text
PROCESSING
     │
     ▼
FAILED
```

Retry:

```text
FAILED
   │
   ▼
PROCESSING
```

Deletion:

```text
READY
   │
   ▼
DELETING
   │
   ▼
DELETED
```

The File Service tracks these states but does not perform the processing itself.

---

# 12. Processing Status

The File Service exposes the current document status.

```http
GET /files/{document_id}/status
```

Response:

```json
{
  "document_id": "doc_789",
  "status": "PROCESSING"
}
```

Possible states:

```text
UPLOADING
UPLOADED
PROCESSING
READY
FAILED
DELETING
DELETED
```

This allows clients to know whether a document is ready for use.

---

# 13. Permissions

File Service owns file-level access control.

Before accessing a document:

```text
Request
   ↓
File Service
   ↓
Authentication / Authorization
   ↓
Permission Check
   ↓
Allow / Reject
```

Permissions can be maintained through:

```text
file_permissions
```

Operations requiring authorization include:

* Metadata access
* Download
* Retry
* Delete
* Sharing/access changes

---

# 14. Download

```http
GET /files/{document_id}/download
```

Flow:

```text
Client
   ↓
File Service
   ↓
Permission Check
   ↓
Generate Signed URL
   ↓
S3 / MinIO
```

The actual file transfer can happen directly from object storage.

---

# 15. Retry

If downstream processing fails, the File Service exposes:

```http
POST /files/{document_id}/retry
```

The File Service:

```text
Validate document
      ↓
Check FAILED state
      ↓
Reset processing state
      ↓
Publish retry/processing event
```

The original file does not need to be uploaded again.

---

# 16. Delete

```http
DELETE /files/{document_id}
```

Flow:

```text
Permission Check
      ↓
DELETING
      ↓
Delete S3/MinIO object
      ↓
Update PostgreSQL
      ↓
Trigger downstream cleanup
      ↓
DELETED
```

The File Service is responsible for the original file and its metadata. Downstream indexed representations must also be cleaned up through the appropriate event/process.

---

# 17. File Versioning

The File Service can maintain multiple versions of a document.

Example:

```text
document_id = doc_789

v1 → physics.pdf
v2 → physics_updated.pdf
```

`file_versions` can contain:

```text
document_id
version
storage_key
checksum
size_bytes
created_at
```

This allows processing and storage to be associated with a specific file version.

---

# 18. Idempotency

Upload completion and event handling must be idempotent.

For example, if:

```text
POST /files/doc_789/complete
```

is accidentally sent twice, the File Service should not create duplicate file records or duplicate processing requests.

Use identifiers such as:

```text
document_id
event_id
version
```

for idempotency.

The GraphGPT event architecture requires idempotent processing for duplicate events. 

---

# 19. Reliable Event Publishing

Potential failure:

```text
PostgreSQL
    ↓
File marked UPLOADED

Kafka
    X
event not published
```

This creates an inconsistent state.

Use the **Transactional Outbox Pattern**:

```text
                PostgreSQL
        ┌──────────────────────┐
        │ files                │
        │ outbox_events        │
        └──────────┬───────────┘
                   │
                   ▼
              Outbox Worker
                   │
                   ▼
                 Kafka
```

The database transaction stores both:

```text
File state change
+
Event to be published
```

The event publisher then reliably delivers the event to Kafka.

The broader GraphGPT architecture identifies the Outbox Pattern for reliable event publishing. 

---

# 20. API Design

| Method   | Endpoint                        | Purpose               |
| -------- | -------------------------------- | ---------------------- |
| `POST`   | `/files/upload/initiate`        | Initialize upload     |
| `POST`   | `/files/{document_id}/complete` | Complete upload       |
| `GET`    | `/files/{document_id}`          | Get metadata          |
| `GET`    | `/files`                        | List files            |
| `GET`    | `/files/{document_id}/download` | Download/access       |
| `GET`    | `/files/{document_id}/status`   | Get processing status |
| `POST`   | `/files/{document_id}/retry`    | Retry processing      |
| `DELETE` | `/files/{document_id}`          | Delete file           |

---

# 21. Scalability

The File Service should be horizontally scalable and stateless.

```text
                    Load Balancer
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          File Pod    File Pod    File Pod
              │          │          │
              └──────────┼──────────┘
                         │
                ┌────────┴────────┐
                ▼                 ▼
           PostgreSQL          S3/MinIO
                         │
                         ▼
                       Kafka
```

Direct-to-object-storage uploads ensure that large file transfers do not consume File Service application resources.

---

# 22. Failure Handling

| Failure                  | File Service Handling              |
| ------------------------ | ----------------------------------- |
| Invalid file             | Reject upload                      |
| S3 upload failure        | Client retries                     |
| Duplicate completion     | Idempotency                        |
| PostgreSQL failure       | Retry/failover                     |
| Kafka unavailable        | Outbox/retry                       |
| Processing failure       | Track `FAILED`                     |
| Retry requested          | Republish processing event         |
| File deletion            | Update lifecycle + cleanup trigger |
| Service instance failure | Stateless restart                  |

Kafka-based asynchronous processing also provides retries, backoff, partitioning and DLQ capabilities. 

---

# 23. Security

File Service should enforce:

* Authentication
* Authorization
* File ownership
* File-type validation
* File-size limits
* Secure object-storage access
* Presigned URL expiration
* No direct public bucket access
* Input validation
* Malware/security scanning where required

The File Service should never expose permanent public S3/MinIO credentials or unrestricted object paths to clients.

---

# 24. Observability

The service should expose:

### Metrics

```text
upload_requests_total
upload_failures_total
upload_duration
file_size_bytes
processing_status_count
download_requests_total
delete_requests_total
```

### Logs

Every request/event should be traceable using:

```text
request_id
document_id
user_id
event_id
```

### Tracing

Distributed tracing should cover:

```text
BFF
 ↓
File Service
 ↓
PostgreSQL / S3
 ↓
Kafka
```

---

# 25. Final File Service Boundary

```text
                 FILE SERVICE
                      │
       ┌──────────────┼──────────────┐
       │              │              │
       ▼              ▼              ▼
    Upload         Metadata       Permissions
       │              │              │
       ▼              ▼              ▼
   S3/MinIO      PostgreSQL      PostgreSQL
       │
       ▼
  Lifecycle / Status
       │
       ▼
 file.uploaded
       │
       ▼
     Kafka
```

### The key principle

> **File Service manages the file, not the document intelligence.**

It owns:

**Upload → Storage → Metadata → Permissions → Lifecycle → Event**

After `file.uploaded`, downstream processing takes over. The File Service does **not** parse, chunk, embed, search, rerank, or generate LLM responses.

:::
