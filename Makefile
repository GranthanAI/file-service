.PHONY: help sync infra infra-down db-create db-migrate db-revision minio-init run test clean

all: help

help:
	@echo "Available commands:"
	@echo "  make sync        - Install and sync virtual environment packages using uv"
	@echo "  make infra       - Start shared GraphGPT docker containers (Postgres, Kafka, file-minio, etc.)"
	@echo "  make infra-down  - Stop the shared GraphGPT docker containers"
	@echo "  make db-create   - Idempotently create the file_service_db database (safe on an already-initialized Postgres volume)"
	@echo "  make db-migrate  - Apply Alembic migrations (uv run alembic upgrade head) — see README if this hangs/errors, re: port 5432 conflicts"
	@echo "  make db-revision - Autogenerate a new Alembic revision (make db-revision MSG=\"description\")"
	@echo "  make minio-init  - Idempotently create/configure the graphgpt-files(-quarantine) buckets"
	@echo "  make run         - Launch FastAPI application dev server with auto-reload"
	@echo "  make test        - Run unit tests using pytest"
	@echo "  make clean       - Remove virtual environment (.venv) and byte caches"

sync:
	uv sync

infra:
	docker compose -f ../docker-compose.yml up -d

infra-down:
	docker compose -f ../docker-compose.yml down

# Idempotent regardless of whether docker-entrypoint-initdb.d already ran
# (i.e. works even on a pre-existing postgres_data volume).
db-create:
	docker exec graphgpt-postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'file_service_db'" | grep -q 1 || \
	docker exec graphgpt-postgres psql -U postgres -c "CREATE DATABASE file_service_db"

# Standard host-based path (matches `make run` / `make test`) — requires
# .env's DATABASE_URL to actually reach the graphgpt-postgres container. If
# this errors with "database ... does not exist" even though `make db-create`
# succeeded, something else on this machine is answering on port 5432 ahead
# of Docker's forward (e.g. a native/local Postgres install) — see the
# troubleshooting note in README.md rather than reaching for a container.
db-migrate:
	uv run alembic upgrade head

db-revision:
	uv run alembic revision --autogenerate -m "$(MSG)"

# Runs docker/minio/init-buckets.sh inside a throwaway minio/mc container on
# the shared graphgpt-net network — no local `mc` binary required. Targets
# file-service's own dedicated `file-minio` container (root docker-compose.yml).
# MSYS_NO_PATHCONV=1 avoids Git-Bash-on-Windows mangling `/bin/sh` and
# `/init-buckets.sh` into Windows paths; harmless/unused on Linux and macOS.
minio-init:
	MSYS_NO_PATHCONV=1 docker run --rm --network graphgpt-net \
		-e MINIO_URL=http://file-minio:9000 \
		-e MINIO_ROOT_USER=minioadmin \
		-e MINIO_ROOT_PASSWORD=minioadmin123 \
		-e BUCKET_FILES=graphgpt-files \
		-e BUCKET_QUARANTINE=graphgpt-files-quarantine \
		-v "$(CURDIR)/docker/minio/init-buckets.sh:/init-buckets.sh:ro" \
		--entrypoint /bin/sh \
		minio/mc:RELEASE.2024-01-13T08-44-48Z /init-buckets.sh

run:
	uv run uvicorn app.main:app --reload --host 127.0.0.1 --port 8006

test:
	uv run pytest

clean:
	rm -rf .venv
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
