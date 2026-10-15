
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from app.config import settings
from app.services.errors import ConflictError


class KeyedLock:
    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    @contextmanager
    def hold(self, key: str) -> Iterator[None]:
        with self._guard:
            lock = self._locks.setdefault(key, threading.Lock())
        with lock:
            yield


class RedisKeyedLock:
    def __init__(self, redis_url: str, timeout: float = 60, wait: float = 30) -> None:
        import redis

        self._redis = redis.Redis.from_url(redis_url)
        self.timeout = timeout
        self.wait = wait

    @contextmanager
    def hold(self, key: str):
        lock = self._redis.lock(f"lock:subscription:{key}", timeout=self.timeout, blocking_timeout=self.wait)
        if not lock.acquire():
            raise ConflictError(f"subscription {key} is busy, try again")
        try:
            yield
        finally:
            try:
                lock.release()
            except Exception:
                pass


def build_lock():
    return RedisKeyedLock(settings.redis_url) if settings.use_celery else KeyedLock()


subscription_locks = build_lock()
