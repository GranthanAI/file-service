from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration, loaded from environment variables / .env.

    Field names mirror the configuration table in docs/LLD.md §7.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # App
    APP_NAME: str = "file-service"
    APP_ENV: str = "development"
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    LOG_LEVEL: str = "INFO"

    # Database
    DATABASE_URL: str
    DB_ECHO: bool = False
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 10

    # MinIO
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_USE_TLS: bool = False
    BUCKET_FILES: str = "graphgpt-files"
    BUCKET_QUARANTINE: str = "graphgpt-files-quarantine"
    MINIO_KAFKA_NOTIFICATION_ARN: str = "arn:minio:sqs::file-events:kafka"
    # Whether to wire the bucket -> Kafka notification during bootstrap (docs/LLD.md §3.2,
    # docs/HLD.md §12). Requires the `notify_kafka:file-events` target to already be
    # registered server-side (`mc admin config set ...`) — separate infra setup this app
    # cannot perform itself, so this defaults off until that's provisioned.
    MINIO_NOTIFICATIONS_ENABLED: bool = False
    # docs/HLD.md §12 example topic name — must match whatever the MinIO
    # server-side `notify_kafka:file-events` target was actually provisioned
    # with (see docker/minio/init-buckets.sh), since that's server-side
    # config this app doesn't control.
    MINIO_OBJECT_EVENTS_TOPIC: str = "minio.object.events"
    # docs/HLD.md §6.3 lifecycle rule — how long an object may sit in
    # BUCKET_QUARANTINE unpromoted before MinIO's own lifecycle rule expires
    # it (bucket_bootstrap.py), preventing orphaned quarantined files from
    # accumulating.
    QUARANTINE_OBJECT_EXPIRY_DAYS: int = 1

    # Kafka
    KAFKA_BROKERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP_ID: str = "file-service-consumers"
    # Shared by processing_status_consumer.py and quarantine_service.py's
    # consumer loops — how long each getmany() poll waits and how many
    # records it pulls per poll.
    KAFKA_CONSUMER_POLL_TIMEOUT_MS: int = 1000
    KAFKA_CONSUMER_MAX_RECORDS: int = 50

    # Upload / presign policy
    UPLOAD_PRESIGN_TTL_SECONDS: int = 900
    DOWNLOAD_PRESIGN_TTL_SECONDS: int = 300
    MAX_UPLOAD_SIZE_BYTES: int = 104_857_600
    ALLOWED_MIME_TYPES: str = "application/pdf,text/plain"
    UPLOAD_STRICT_POLICY: bool = True

    # docs/LLD.md §5.4 GET /files pagination — the client-facing default/max
    # page size.
    FILE_LIST_DEFAULT_PAGE_SIZE: int = 20
    FILE_LIST_MAX_PAGE_SIZE: int = 100

    # Feature flags
    AV_SCAN_ENABLED: bool = False
    # docs/LLD.md Phase 6 task 4: a pluggable scanner interface — CLAMAV_ENABLED=false
    # (the default, for environments without ClamAV) uses av_scanner.NoOpScanner,
    # which always reports clean; true uses av_scanner.ClamdScanner against a
    # real clamd daemon at CLAMAV_HOST:CLAMAV_PORT.
    CLAMAV_ENABLED: bool = False
    CLAMAV_HOST: str = "localhost"
    CLAMAV_PORT: int = 3310
    CLAMAV_TIMEOUT_SECONDS: float = 30.0

    # Background workers
    OUTBOX_POLL_INTERVAL_MS: int = 500
    # docs/LLD.md §4.7 — how many outbox_events rows one poll-and-publish
    # cycle fetches/publishes at most (the `LIMIT` in the SKIP LOCKED query).
    OUTBOX_BATCH_SIZE: int = 100
    RECONCILIATION_INTERVAL_SECONDS: int = 300
    # docs/LLD.md §4.6 step 3: how long an ObjectCreated event is left
    # unevaluated before being cross-checked against `files` — avoids
    # flagging a MinIO event as orphaned while the client still has a fair,
    # normal-timing chance to call /complete.
    RECONCILIATION_ORPHAN_GRACE_SECONDS: int = 600
    # Reconciliation's own MinIO-notification poll — kept separate from
    # KAFKA_CONSUMER_POLL_TIMEOUT_MS/MAX_RECORDS above since its batch size
    # (200) is deliberately larger than the live consumers' (50): it only
    # runs once per RECONCILIATION_INTERVAL_SECONDS, not continuously, so a
    # bigger per-sweep batch drains the backlog faster without adding
    # continuous load.
    RECONCILIATION_POLL_TIMEOUT_MS: int = 1000
    RECONCILIATION_MAX_RECORDS: int = 200
    # A distinct consumer group from KAFKA_CONSUMER_GROUP_ID so the
    # reconciliation worker's offsets never interfere with quarantine_service's
    # own consumption of the same settings.MINIO_OBJECT_EVENTS_TOPIC.
    RECONCILIATION_CONSUMER_GROUP_ID: str = "file-service-consumers-reconciliation"

    # docs/LLD.md §4.8 — how long GET /health/ready waits on each dependency
    # check before treating it as failed; a hung DB/MinIO must not hang the
    # readiness probe itself.
    HEALTH_CHECK_TIMEOUT_SECONDS: float = 3.0

    # Auth — verifies JWTs issued by auth-service locally (no network call,
    # no DB dependency), matching auth-service's OWN actual implementation
    # (app/security/jwt_manager.py: HS256 with a shared JWT_SECRET_KEY) —
    # NOT the RS256/public-key design its docs describe but its code doesn't
    # implement. Must be the same secret/algorithm auth-service signs with.
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"

    @property
    def allowed_mime_types(self) -> set[str]:
        return {ct.strip() for ct in self.ALLOWED_MIME_TYPES.split(",") if ct.strip()}


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
