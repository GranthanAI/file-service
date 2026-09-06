# Task Complete 1: Project Bootstrap & Database Layer (Phase 0 + Phase 1)

This document provides a detailed log of the scaffolding, infrastructure wiring, and database-layer implementation completed for the **Granthan File Service**, covering **Phase 0 (Project Bootstrap & Local Infrastructure)** and **Phase 1 (Database Layer)** of [docs/IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md).

---

## 1. What Was Done

### 1.1 Phase 0 — Project Bootstrap & Local Infrastructure

1. **Directory Structure & Boilerplates:** Scaffolded the full `app/` layout from [docs/LLD.md §1](../LLD.md#1-directory-to-module-mapping-where-why-and-how) — `api/`, `services/`, `repositories/`, `models/`, `storage/`, `validation/`, `events/`, `db/`, `core/`, `workers/`, `utils/` — with empty `__init__.py` placeholders for everything not yet implemented, so the module layout matches the LLD from day one instead of growing ad hoc.
2. **Dependency Management:** [pyproject.toml](file:///c:/Users/hp/Desktop/Granthan/file-service/pyproject.toml) declares FastAPI, SQLAlchemy (async) + asyncpg, Alembic, `minio`, `aiokafka`, `structlog`, `filetype`, plus a dev group (`pytest`, `pytest-asyncio`, `httpx`, `testcontainers[postgres,kafka]`) — synced via `uv`, matching the toolchain `auth-service`/`conversation-service` already use.
3. **Configuration:** [config.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/core/config.py) — a single Pydantic `Settings` class covering every field from [docs/LLD.md §7](../LLD.md#7-configuration-appcoreconfigpy) (DB URL, MinIO endpoint/keys/buckets, Kafka brokers, presign TTLs, upload limits, `AV_SCAN_ENABLED` feature flag).
4. **Structured Logging:** [logging.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/core/logging.py) — `structlog` configured for JSON output, ready to carry `request_id`/`document_id`/`owner_id`/`event_id` fields in later phases.
5. **App Bootstrap:** [main.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/main.py) + [lifespan.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/lifespan.py) — app factory wiring the DB engine ([db/postgres.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/db/postgres.py), [db/session.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/db/session.py)) and a process-scoped MinIO client ([storage/minio_client.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/storage/minio_client.py)) at startup.
6. **Health Endpoint:** [health.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/routers/health.py) — `GET /health/live`, liveness only (no dependency calls), matching the liveness/readiness split later formalized in Phase 10.
7. **Root Infrastructure Update:** Extended the **shared** [../../docker-compose.yml](file:///c:/Users/hp/Desktop/Granthan/docker-compose.yml) — reused the existing `postgres` and `kafka` services as-is (only addition: an init-script mount so `file_service_db` is created automatically on a fresh volume), and added one **new, dedicated** `file-minio` service (same already-pulled `minio/minio` image tag as `milvus-minio`, but a separate container/volume/ports, since file-service's buckets shouldn't share lifecycle with Milvus's internal storage).
8. **Local Tooling:** [docker/postgres/init-file-service-db.sql](file:///c:/Users/hp/Desktop/Granthan/file-service/docker/postgres/init-file-service-db.sql), [docker/minio/init-buckets.sh](file:///c:/Users/hp/Desktop/Granthan/file-service/docker/minio/init-buckets.sh), and `Makefile` targets (`infra`, `db-create`, `minio-init`, `run`, `test`) — all idempotent, safe to re-run.
9. **Dockerfile:** Multi-stage `uv`-based build, kept for actual deployment (Phase 13) — **not** wired into the everyday dev loop, matching `auth-service`'s convention of running the app locally via `uv run uvicorn` rather than containerized.

### 1.2 Phase 1 — Database Layer

1. **Enums:** [core/enums.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/core/enums.py) — `FileStatus` (including `SCANNING`), `PermissionLevel`, `PrincipalType`, `UploadJobStatus`, `PresignMethod`.
2. **Models:** [models/file.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/models/file.py), [upload_job.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/models/upload_job.py), [file_version.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/models/file_version.py), [file_permission.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/models/file_permission.py), [outbox.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/models/outbox.py) — all 5 tables from [docs/LLD.md §2](../LLD.md#2-database-design--schema-details), including `retry_count`, `last_error_code`, `last_error_message`, the `etag`/`checksum_sha256` split, and every index/constraint (partial `idx_files_status`, unique `uq_files_bucket_key`, `CHECK size_bytes >= 0`, partial `idx_upload_jobs_pending_expiry`, partial `idx_outbox_unprocessed`).
3. **Declarative Base:** [db/base.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/db/base.py) now imports all 5 model modules so Alembic autogenerate can see them.
4. **Lifecycle Service (pulled forward from Phase 4):** [services/lifecycle_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/lifecycle_service.py) — the `ALLOWED_TRANSITIONS` state machine and `transition()` guard, needed early because `file_repository.update_status()` delegates to it, per the LLD's own design.
5. **Exceptions:** [core/exceptions.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/core/exceptions.py) — minimal `FileServiceError` base + `InvalidStateTransitionError` (the rest of the set arrives in Phase 3).
6. **Repositories:** [file_repository.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/repositories/file_repository.py), [upload_job_repository.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/repositories/upload_job_repository.py), [file_version_repository.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/repositories/file_version_repository.py), [file_permission_repository.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/repositories/file_permission_repository.py), [outbox_repository.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/repositories/outbox_repository.py) — matching `auth-service`'s `@staticmethod` repository style, exposing only the methods each future service actually needs.
7. **ID Generator:** [utils/id_generator.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/utils/id_generator.py) — hand-rolled `new_uuid7()` (RFC 9562), no extra dependency (Python 3.12 predates the stdlib `uuid.uuid7()` added in 3.14).
8. **Alembic Migration:** Autogenerated against a live Postgres, then hand-corrected for a real gap — `51a8645188fe_create_file_service_tables.py` — see [§3.2](#32-fixing-the-alembic-downgrade-enum-gap) below.
9. **Tests:**
   * [tests/unit/test_id_generator.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_id_generator.py) — version/variant checks, ordering, uniqueness.
   * [tests/unit/test_lifecycle_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_lifecycle_service.py) — every allowed edge exercised, plus explicit illegal-edge regression cases (including the `SCANNING → PROCESSING` edge that a design review earlier caught and fixed).
   * [tests/integration/conftest.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/conftest.py) + [test_file_repository.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_file_repository.py) — a real, ephemeral Postgres via `testcontainers`, migrated with the actual Alembic chain, exercising insert/get/keyset-pagination/retry-count/soft-delete-filtering and confirming the partial index exists.

---

## 2. Why It Was Done

* **Scaffolding the full tree upfront (Phase 0):** Matches how the LLD documents the codebase — later phases fill in files that already exist as empty placeholders in the right place, rather than the structure drifting from the design doc as work progresses.
* **Dedicated `file-minio` instead of reusing `milvus-minio`:** Even though `milvus-minio` was already running and would have avoided a second container, sharing it would couple file-service's availability and bucket namespace to Milvus's internal storage lifecycle — a MinIO restart/reconfig for Milvus shouldn't be able to take down file uploads. It still reuses the exact same image tag already pulled, so nothing new was downloaded.
* **App not containerized for local dev:** Every other GraphGPT service in this repo (`auth-service`, `memory-service`, etc.) runs its app via `uv run uvicorn` locally, with Docker reserved for shared infra. Containerizing file-service's own app for dev — which was tried and reverted twice during this work — broke that consistency for no real benefit locally (slower reload, an extra 477MB image with no counterpart in any other service).
* **`lifecycle_service.py` pulled from Phase 4 into Phase 1:** The LLD explicitly specifies `file_repository.update_status()` as delegating to `lifecycle_service.transition()` before writing `files.status` — implementing a fake/unvalidated `update_status` now and redoing it correctly in Phase 4 would mean writing the same logic twice for no reason, since the function is small, pure Python, and has no HTTP/MinIO dependency (i.e., it doesn't violate Phase 1's "independent of any HTTP or MinIO logic" goal).
* **UUIDv7 for `document_id`, generated in-app rather than `gen_random_uuid()`:** `document_id` is already the primary key with its own B-tree index, so keyset pagination works regardless of UUID version — what UUIDv7 adds on top is that it's time-sortable, so `ORDER BY document_id` doubles as a chronological cursor without a second index on `created_at` (see [docs/LLD.md §2.1](../LLD.md#21-files-table) for the corrected reasoning after an earlier review flagged the original explanation as backwards).
* **Client-side `uuid.uuid4()` defaults for surrogate PKs instead of `gen_random_uuid()`:** The LLD's SQL DDL shows `DEFAULT gen_random_uuid()` for `upload_jobs.id`, `file_versions.id`, etc. Using SQLAlchemy's Python-side `default=uuid.uuid4` instead avoids requiring the `pgcrypto` extension and matches `auth-service`'s existing model convention exactly — functionally equivalent, one less moving part.
* **`testcontainers` for the Phase 1 integration test, not the shared dev Postgres:** This machine has a native Windows `postgres.exe` service also bound to port 5432 (see [§4](#4-a-detour-two-docker-image-mistakes-and-a-networking-gotcha) below) — `testcontainers` sidesteps that entirely by binding to a random free port, and it's the same mechanism the LLD's own testing strategy (Phase 12) already calls for.

---

## 3. How It Was Done

### 3.1 Generating and Verifying the Migration

The migration was **autogenerated against a real, running Postgres** (not hand-typed from scratch) to catch any drift between the SQLAlchemy models and the LLD's SQL immediately, then reviewed line-by-line:

```text
docker build -t file-service-tools:local .          # one-off, deleted after use
docker run --rm --network graphgpt-net \
    -e DATABASE_URL=postgresql+asyncpg://postgres:1234@postgres:5432/file_service_db \
    -v "$(pwd)/app/db/migrations/versions:/app/app/db/migrations/versions" \
    --entrypoint alembic file-service-tools:local \
    revision --autogenerate -m "create file service tables"
```

Autogenerate correctly detected every table, every index (including the `postgresql_where` partial-index clauses), the `CHECK` constraint, and both Postgres `ENUM` types. It was then verified with a full round-trip — `upgrade head` → `downgrade -1` → `upgrade head` — to prove the migration is safe to reverse, which is what surfaced the `ENUM`-type gap described below.

### 3.2 Fixing the Alembic Downgrade Enum Gap

Postgres `ENUM` types are separate objects from the tables that use them — `DROP TABLE` does not drop the `TYPE`. Autogenerate's `downgrade()` dropped the tables but left `file_status` and `permission_level` behind, which would break a subsequent `upgrade()` with "type already exists". Verified by checking `pg_type` immediately after a downgrade:

```sql
SELECT typname FROM pg_type WHERE typname IN ('file_status','permission_level');
-- (0 rows) after the fix below was added and downgrade re-run
```

Fixed by appending explicit, `checkfirst=True` drops to `downgrade()` (see [§4.1](#41-the-alembic-downgrade-fix) below for the code).

### 3.3 Integration Test Flow (testcontainers)

```mermaid
sequenceDiagram
    autonumber
    participant Pytest
    participant TC as testcontainers
    participant PG as ephemeral Postgres (random port)
    participant Alembic
    participant Repo as FileRepository

    Pytest->>TC: start PostgresContainer("postgres:16-alpine")
    TC-->>Pytest: connection URL (random host port)
    Pytest->>Alembic: command.upgrade(cfg, "head")
    Alembic->>PG: run full migration chain
    Pytest->>Repo: insert / get / list_paginated / increment_retry_count
    Repo->>PG: SQL via AsyncSession
    PG-->>Repo: rows
    Pytest->>Pytest: assertions
    Pytest->>PG: session.rollback() (test isolation)
```

This runs the **real** migration chain, not a hand-rolled `create_all()` — so a schema bug in the Alembic migration itself would be caught by these same tests, not just a model-level mismatch.

---

## 4. A Detour: Two Docker Image Mistakes (and a Networking Gotcha)

Worth documenting explicitly since it shaped several of the decisions above:

1. **First mistake:** Containerized the file-service app itself (`docker compose up` building `file-service-file-service:latest`, 477MB) after initially reading the phase spec literally ("`docker compose up` brings up Postgres + MinIO + Kafka + app"). This didn't match any other service in the repo — `auth-service`'s own `docker-compose.yml` is fully commented out, app run via `uv run uvicorn`. Reverted: image deleted, `docker-compose.yml` turned into a commented-out reference file matching `auth-service` exactly, `Makefile` trimmed back to `sync/infra/infra-down/db-create/minio-init/run/test/clean`.
2. **Second mistake:** While generating the Alembic migration, `uv run alembic revision --autogenerate` failed with `database "file_service_db" does not exist` — even though `docker exec graphgpt-postgres psql ...` confirmed the database *did* exist. Root cause: a native Windows `postgres.exe` service was also bound to `0.0.0.0:5432` alongside Docker Desktop's forward for the same port, so host-side connections landed on the wrong Postgres. The workaround — building a second custom image (`file-service-tools:local`) just to run `alembic` inside a container on the Docker network — worked, but repeated the same mistake: a persistent, tagged, custom image left sitting in `docker images` for what should have been a one-off command.
3. **Resolution:** Both images were deleted. `make db-migrate` / `make db-revision` now run `alembic` the standard way (`uv run alembic ...`, matching `make run`/`make test`), and `README.md` documents the port-5432 conflict as a **host environment** problem to diagnose (`tasklist | findstr postgres` / `lsof -i :5432`) and fix at the OS level — not something to route around by containerizing tooling. The Phase 1 integration tests were written against `testcontainers` instead specifically because it avoids this exact conflict (random host port) without introducing any custom image.

The lesson embedded in the tooling now: **only shared infra runs in Docker for local dev; anything file-service-specific (the app, one-off scripts, tests) runs via `uv run` on the host, or via ephemeral/standard images that leave nothing behind (`testcontainers`, `minio/mc` for bucket init).**

---

## 5. Code Highlights & Explanations

### 5.1 The Alembic Downgrade Fix

Located in: [app/db/migrations/versions/51a8645188fe_create_file_service_tables.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/db/migrations/versions/51a8645188fe_create_file_service_tables.py)

```python
def downgrade() -> None:
    # ... auto-generated table/index drops ...

    # Not auto-generated: dropping a table does NOT drop the Postgres ENUM
    # types its columns used (file_status, permission_level) — without this,
    # a downgrade-then-upgrade cycle fails with "type ... already exists".
    sa.Enum(name='file_status').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='permission_level').drop(op.get_bind(), checkfirst=True)
```
* **Why it matters:** Without this, any environment that ever runs a downgrade (rolling back a bad deploy, local schema resets) would permanently break re-upgrading until someone manually ran `DROP TYPE` — a subtle, easy-to-miss failure mode that autogenerate doesn't warn about.

### 5.2 Lifecycle Transition Guard — the No-Op-on-Same-State Rule

Located in: [app/services/lifecycle_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/lifecycle_service.py)

```python
def transition(current: FileStatus, target: FileStatus) -> None:
    if current == target:
        return
    if target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise InvalidStateTransitionError(current, target)
```
* **Why it matters:** Kafka delivery in this system is at-least-once (docs/LLD.md §3.7) — a duplicate `file.processing.completed` event redelivered for a file already `READY` must be a safe no-op, not a crash. This one `if` is what makes every future event consumer safe under redelivery without each of them needing their own idempotency check for this specific case.

### 5.3 `update_status` as the Only Writer of `files.status`

Located in: [app/repositories/file_repository.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/repositories/file_repository.py)

```python
@staticmethod
async def update_status(db: AsyncSession, file_obj: File, target: FileStatus, **fields) -> File:
    lifecycle_service.transition(file_obj.status, target)
    file_obj.status = target
    for field, value in fields.items():
        setattr(file_obj, field, value)
    db.add(file_obj)
    await db.flush()
    return file_obj
```
* **Why it matters:** By routing every status change through this one method (which validates the edge first), no future service — upload, quarantine, retry, or the processing-status consumer — can accidentally set an illegal state by writing `file_obj.status = ...` directly. The `**fields` kwarg lets a single call also set `etag`, `last_error_code`, etc. in the same write, keeping status transitions and their associated data atomic within one Python statement (still inside the caller's DB transaction).

### 5.4 UUIDv7 Bit Layout

Located in: [app/utils/id_generator.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/utils/id_generator.py)

```python
def new_uuid7() -> uuid.UUID:
    unix_ts_ms = int(time.time() * 1000)
    rand_a = int.from_bytes(os.urandom(2), "big") & _VERSION_MASK
    rand_b = int.from_bytes(os.urandom(8), "big") & _VARIANT_MASK

    value = (unix_ts_ms & 0xFFFFFFFFFFFF) << 80
    value |= (_VERSION_BITS | rand_a) << 64
    value |= _VARIANT_BITS | rand_b

    return uuid.UUID(int=value)
```
* **Why it matters:** Implemented by hand (RFC 9562) instead of adding a dependency, since Python 3.12 predates the stdlib `uuid.uuid7()` (added in 3.14). The 48-bit millisecond timestamp occupies the top bits of the 128-bit integer, which is exactly what makes `document_id` values sort chronologically as plain UUID comparisons — the property `list_paginated`'s keyset pagination relies on.

---

## 6. Verification

* `alembic upgrade head` → `downgrade -1` → `upgrade head`: clean round-trip against the real `graphgpt-postgres` container, confirmed via `\d files` and a `pg_type` check.
* `uv run pytest`: **33 passed** (27 unit, 6 integration), zero warnings beyond pre-existing unrelated FastAPI/Starlette deprecations.
* `docker images`: confirmed clean of any file-service-specific image after cleanup — only the standard `postgres:16-alpine` / `minio/minio` images pulled for shared infra remain.
