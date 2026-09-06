# File Service

Granthan's File Service — upload, secure storage (MinIO), metadata (PostgreSQL), permissions, and lifecycle management for user-uploaded files, with asynchronous handoff to Retrieval Service via Kafka.

See [`docs/HLD.md`](./docs/HLD.md) for the high-level design, [`docs/LLD.md`](./docs/LLD.md) for the low-level design, and [`docs/IMPLEMENTATION_PLAN.md`](./docs/IMPLEMENTATION_PLAN.md) for the phased build plan this codebase follows.

## Stack

FastAPI · SQLAlchemy (async) + PostgreSQL · MinIO · aiokafka · Alembic · `uv`

## Local development

Shared infrastructure (Postgres, Kafka — reused from other services — and this service's own dedicated MinIO instance) lives in the **root** [`../docker-compose.yml`](../docker-compose.yml), alongside the rest of GraphGPT's shared containers.

```bash
# 1. One-time / whenever infra is down: start shared containers
make infra

# 2. One-time: create the file_service_db database
#    (only needed if the shared postgres_data volume already existed before
#    this service was added — otherwise it's created automatically)
make db-create

# 3. One-time: create the graphgpt-files / graphgpt-files-quarantine buckets
make minio-init

# 4. Install dependencies
make sync
cp .env.example .env

# 5. Apply database migrations
make db-migrate

# 6. Run the app locally (dev, auto-reload) — matches every other GraphGPT
#    service in this repo: the app runs via `uv run uvicorn`, not
#    containerized, for fast reload
make run
# -> http://127.0.0.1:8006/health/live
```

The `Dockerfile` is still built and used for actual deployment (see `docs/IMPLEMENTATION_PLAN.md` Phase 13) — it's just not part of the everyday local dev loop.

### Troubleshooting: `make db-migrate` says the database doesn't exist

If `make db-create` succeeds but `make db-migrate` (or anything else run via `uv run` that talks to Postgres) fails with `database "file_service_db" does not exist`, something *other than* the `graphgpt-postgres` container is answering on `localhost:5432` — most commonly a native/locally-installed PostgreSQL service on the host, which can silently win over Docker's port forward for the same port. Check with:

```bash
# Windows
tasklist | findstr postgres
# macOS/Linux
lsof -i :5432
```

If a non-Docker `postgres` process shows up, that's the cause. Fix it by stopping that service (or reconfiguring it to a different port) — don't work around it by containerizing file-service's own tooling; that's a separate, real Postgres instance, not something to route file-service through Docker to avoid.

### Ports (local dev)

| Service | Port(s) |
|---|---|
| file-service API | `8006` |
| PostgreSQL (shared) | `5432` |
| Kafka (shared) | `9092` |
| file-service MinIO API | `9010` |
| file-service MinIO Console | `9011` |

Note: the root compose also runs a separate MinIO instance (`milvus-minio`, ports `9000`/`9001`) dedicated to Milvus — that one is unrelated to file-service.

## Tests

```bash
make test
```
