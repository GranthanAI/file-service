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

    # Background workers
    OUTBOX_POLL_INTERVAL_MS: int = 500
    RECONCILIATION_INTERVAL_SECONDS: int = 300

    # Auth
    AUTH_JWT_PUBLIC_KEY_URL: str | None = None

    @property
    def allowed_mime_types(self) -> set[str]:
        return {ct.strip() for ct in self.ALLOWED_MIME_TYPES.split(",") if ct.strip()}


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
