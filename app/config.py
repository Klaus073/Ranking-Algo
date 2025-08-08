import os
from datetime import timedelta
from typing import Optional

from pydantic import BaseModel


class AppSettings(BaseModel):
    # Core
    environment: str = os.getenv("ENVIRONMENT", "local")
    config_version: str = os.getenv("CONFIG_VERSION", "2025-08-07")

    # Network
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))

    # Security
    webhook_hmac_secret: str = os.getenv("WEBHOOK_HMAC_SECRET", "dev-secret")
    webhook_max_skew_seconds: int = int(os.getenv("WEBHOOK_MAX_SKEW_SECONDS", "300"))

    # Queue / Redis
    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    debounce_ttl_seconds: int = int(os.getenv("DEBOUNCE_TTL_SECONDS", "2"))

    # Database (Postgres)
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@postgres:5432/postgres",
    )

    # Workers
    max_concurrency: int = int(os.getenv("MAX_CONCURRENCY", "32"))

    # Histogram
    histogram_bucket_width: int = int(os.getenv("HISTOGRAM_BUCKET_WIDTH", "5"))
    histogram_num_buckets: int = int(os.getenv("HISTOGRAM_NUM_BUCKETS", "200"))

    # Observability
    enable_metrics: bool = os.getenv("ENABLE_METRICS", "true").lower() == "true"

    @property
    def webhook_skew(self) -> timedelta:
        return timedelta(seconds=self.webhook_max_skew_seconds)


settings = AppSettings()


