import os

# Ensure Settings() can construct during test collection even without a .env
# file present (CI, fresh clones) — required fields get harmless test values.
os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://postgres:1234@localhost:5432/file_service_db_test"
)
os.environ.setdefault("MINIO_ENDPOINT", "localhost:9010")
os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")
