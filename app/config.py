import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./folio.db")

    queue_backend: str = os.getenv("QUEUE_BACKEND", "inprocess").lower()
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/1")
    celery_queue: str = os.getenv("CELERY_QUEUE", "folio")
    cache_ttl_seconds: int = int(os.getenv("CACHE_TTL_SECONDS", "60"))

    rebalance_concurrency: int = int(os.getenv("REBALANCE_CONCURRENCY", "4"))

    broker_latency_ms: int = int(os.getenv("BROKER_LATENCY_MS", "0"))
    broker_failure_rate: float = float(os.getenv("BROKER_FAILURE_RATE", "0"))
    broker_max_attempts: int = int(os.getenv("BROKER_MAX_ATTEMPTS", "3"))

    max_multiplier: int = 100

    @property
    def use_celery(self):
        return self.queue_backend == "celery"

    @property
    def use_cache(self):
        return self.use_celery and self.cache_ttl_seconds > 0


settings = Settings()

if settings.queue_backend not in ("inprocess", "celery"):
    raise RuntimeError(f"QUEUE_BACKEND must be 'inprocess' or 'celery', got {settings.queue_backend!r}")
