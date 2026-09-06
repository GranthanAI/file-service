#!/bin/sh
# Idempotent local bucket bootstrap for file-service's dedicated MinIO
# instance (root docker-compose.yml service `file-minio`) — deliberately
# separate from `milvus-minio`, which is Milvus's own storage backend.
#
# This is the LOCAL-DEV equivalent of app/storage/bucket_bootstrap.py
# (Phase 2), which is what actually runs on every app boot in real
# environments. This script exists so `make minio-init` can stand the
# buckets up without needing the Python app running yet (useful right
# after Phase 0, before Phase 2 lands).
#
# Usage: run via `make minio-init` from file-service/ (see Makefile),
# which invokes this script inside a throwaway `minio/mc` container on the
# shared graphgpt-net network — no local `mc` binary install required.
set -e

MINIO_ALIAS="filesvc"
MINIO_URL="${MINIO_URL:-http://file-minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
BUCKET_FILES="${BUCKET_FILES:-graphgpt-files}"
BUCKET_QUARANTINE="${BUCKET_QUARANTINE:-graphgpt-files-quarantine}"

echo "Waiting for MinIO at ${MINIO_URL}..."
until mc alias set "${MINIO_ALIAS}" "${MINIO_URL}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  sleep 1
done

mc mb --ignore-existing "${MINIO_ALIAS}/${BUCKET_FILES}"
mc mb --ignore-existing "${MINIO_ALIAS}/${BUCKET_QUARANTINE}"

mc anonymous set none "${MINIO_ALIAS}/${BUCKET_FILES}"
mc anonymous set none "${MINIO_ALIAS}/${BUCKET_QUARANTINE}"

mc version enable "${MINIO_ALIAS}/${BUCKET_FILES}"

echo "file-service MinIO buckets ready: ${BUCKET_FILES}, ${BUCKET_QUARANTINE}"
