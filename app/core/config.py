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

    # Kafka
    KAFKA_BROKERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP_ID: str = "file-service-consumers"

    # Upload / presign policy
    UPLOAD_PRESIGN_TTL_SECONDS: int = 900
    DOWNLOAD_PRESIGN_TTL_SECONDS: int = 300
    MAX_UPLOAD_SIZE_BYTES: int = 104_857_600
    ALLOWED_MIME_TYPES: str = "application/pdf,text/plain"
    UPLOAD_STRICT_POLICY: bool = True

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
    RECONCILIATION_INTERVAL_SECONDS: int = 300

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
