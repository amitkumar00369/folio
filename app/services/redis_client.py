import asyncio
import weakref

import redis.asyncio as aioredis

from app.config import settings

_clients = weakref.WeakKeyDictionary()


def get_redis():
    loop = asyncio.get_running_loop()
    client = _clients.get(loop)
    if client is None:
        client = aioredis.Redis.from_url(settings.redis_url)
        _clients[loop] = client
    return client
