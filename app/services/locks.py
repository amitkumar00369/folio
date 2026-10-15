import asyncio
from contextlib import asynccontextmanager

from app.config import settings
from app.services.errors import ConflictError
from app.services.redis_client import get_redis


class KeyedLock:
    def __init__(self):
        self._locks: dict[str, asyncio.Lock] = {}

    @asynccontextmanager
    async def hold(self, key: str):
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            yield


class RedisKeyedLock:
    def __init__(self, timeout: float = 60, wait: float = 30):
        self.timeout = timeout
        self.wait = wait

    @asynccontextmanager
    async def hold(self, key: str):
        lock = get_redis().lock(f"lock:subscription:{key}", timeout=self.timeout, blocking_timeout=self.wait)
        if not await lock.acquire():
            raise ConflictError(f"subscription {key} is busy, try again")
        try:
            yield
        finally:
            try:
                await lock.release()
            except Exception:
                pass


def build_lock():
    return RedisKeyedLock() if settings.use_celery else KeyedLock()


subscription_locks = build_lock()
