import json
import logging

from redis.exceptions import RedisError

from app.config import settings
from app.services.redis_client import get_redis

log = logging.getLogger(__name__)

FOLIOS = "folios"
SUBSCRIPTIONS = "subscriptions"


class RedisCache:
    def __init__(self, ttl: int):
        self.ttl = ttl

    async def _key(self, namespace: str, parts):
        print("namespace",namespace)
        version = await get_redis().get(f"folio:cache-version:{namespace}")
        suffix = ":".join(str(p) for p in parts)
        return f"folio:cache:{namespace}:{int(version or 0)}:{suffix}"

    async def get_or_load(self, namespace: str, parts, loader):
        try:
            key = await self._key(namespace, parts)
            raw = await get_redis().get(key)
        except RedisError as exc:
            log.warning("cache read failed, using db: %s", exc)
            return await loader()

        if raw is not None:
            return json.loads(raw)

        value = await loader()
        try:
            await get_redis().set(key, json.dumps(value), ex=self.ttl)
        except RedisError as exc:
            log.warning("cache write failed: %s", exc)
        return value

    async def invalidate(self, *namespaces: str):
        try:
            for namespace in namespaces:
                await get_redis().incr(f"folio:cache-version:{namespace}")
        except RedisError as exc:
            log.warning("cache invalidate failed: %s", exc)


class NoCache:
    async def get_or_load(self, namespace: str, parts, loader):
        return await loader()

    async def invalidate(self, *namespaces: str):
        pass


def build_cache():
    return RedisCache(settings.cache_ttl_seconds) if settings.use_cache else NoCache()


cache = build_cache()


def get_cache():
    return cache
