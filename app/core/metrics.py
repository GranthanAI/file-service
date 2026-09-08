"""docs/HLD.md §21 — the Prometheus metric set, defined once here so every
service/router/worker that needs to record one imports the same instance
(the `prometheus_client` registry is process-global; defining a metric twice
raises `ValueError: Duplicated timeseries`, so this module is the single
place these get constructed).

Exposed on `GET /metrics` by app/api/routers/health.py (docs/LLD.md Phase 10
task 1-2) via `prometheus_client.generate_latest(REGISTRY)`.
"""

from prometheus_client import Counter, Histogram

upload_requests_total = Counter("upload_requests_total", "Total upload initiate requests")
upload_failures_total = Counter("upload_failures_total", "Total upload failures", ["reason"])
upload_duration_seconds = Histogram("upload_duration_seconds", "Time spent in the upload initiate/complete handlers")

minio_stat_object_duration_seconds = Histogram(
    "minio_stat_object_duration_seconds", "Time spent in MinIO StatObject calls (object_verifier)"
)
minio_presign_duration_seconds = Histogram(
    "minio_presign_duration_seconds", "Time spent generating a presigned URL/POST policy"
)

file_size_bytes = Histogram(
    "file_size_bytes", "Size of files uploaded", buckets=(1_024, 65_536, 1_048_576, 10_485_760, 104_857_600)
)
# docs/HLD.md §21 names this "processing_status_count" (no _total suffix),
# but prometheus_client auto-appends "_total" to every Counter's exposed
# series name when the given name doesn't already end with it — so the
# actual scraped metric is processing_status_count_total{status=...}.
processing_status_count = Counter("processing_status_count", "File status transitions observed", ["status"])

download_requests_total = Counter("download_requests_total", "Total download URL requests")
delete_requests_total = Counter("delete_requests_total", "Total delete requests")

outbox_publish_lag_seconds = Histogram(
    "outbox_publish_lag_seconds", "Time between an outbox_events row's created_at and its successful publish"
)
reconciliation_orphans_total = Counter(
    "reconciliation_orphans_total", "MinIO objects observed with no matching non-UPLOADING files row past the grace period"
)
