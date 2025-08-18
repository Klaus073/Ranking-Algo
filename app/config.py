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

    # Security (unused in simplified setup)
    webhook_hmac_secret: str = os.getenv("WEBHOOK_HMAC_SECRET", "")
    webhook_max_skew_seconds: int = int(os.getenv("WEBHOOK_MAX_SKEW_SECONDS", "300"))

    # Queue / Redis (default to local on Windows/dev)
    redis_url: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    debounce_ttl_seconds: int = int(os.getenv("DEBOUNCE_TTL_SECONDS", "45"))
    registration_debounce_ttl_seconds: int = int(os.getenv("REGISTRATION_DEBOUNCE_TTL_SECONDS", "5"))
    # Cache
    score_cache_ttl_seconds: int = int(os.getenv("SCORE_CACHE_TTL_SECONDS", "86400"))

    # Database (Postgres)
    # Note: asyncpg expects plain 'postgresql://' or 'postgres://'. Leave empty to disable direct DB.
    database_url: str = os.getenv("DATABASE_URL", "")

    # Workers
    max_concurrency: int = int(os.getenv("MAX_CONCURRENCY", "32"))

    # Histogram
    histogram_bucket_width: int = int(os.getenv("HISTOGRAM_BUCKET_WIDTH", "5"))
    histogram_num_buckets: int = int(os.getenv("HISTOGRAM_NUM_BUCKETS", "200"))

    # Observability
    enable_metrics: bool = os.getenv("ENABLE_METRICS", "true").lower() == "true"

    # Supabase client (for reads)
    supabase_url: Optional[str] = os.getenv("SUPABASE_URL")
    supabase_anon_key: Optional[str] = os.getenv("SUPABASE_ANON_KEY")
    supabase_service_key: Optional[str] = os.getenv("SUPABASE_SERVICE_KEY")

    @property
    def webhook_skew(self) -> timedelta:
        return timedelta(seconds=self.webhook_max_skew_seconds)


settings = AppSettings()


