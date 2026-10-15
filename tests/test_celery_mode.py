import asyncio

import pytest

from app.config import settings
from app.db.database import SessionLocal
from app.models import RebalanceTask
from app.services.broker_service import MockBroker
from app.services.cache import RedisCache
from app.services.errors import ConflictError
from app.services.locks import RedisKeyedLock
from app.services.rebalance_service import RebalanceProcessor
from app.workers.celery_app import CeleryDispatcher, celery_app
from tests.conftest import base_quantities, orders_for, subscribe, trigger_job


@pytest.fixture
def eager_celery(client):
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True
    previous = client.app.state.rebalance_worker
    client.app.state.rebalance_worker = CeleryDispatcher()
    yield
    client.app.state.rebalance_worker = previous
    celery_app.conf.task_always_eager = False


def test_cascade_through_celery_tasks(client, eager_celery):
    base = base_quantities(client)["RELIANCE"]
    subs = {u: subscribe(client, u, m)["subscription"]["id"] for u, m in [("a", 1), ("b", 3), ("c", 5)]}

    r = client.post(
        "/api/admin/rebalance",
        json={"folio_id": "folio-1", "outgoing_ticker": "RELIANCE", "incoming_ticker": "IDEA"},
    )
    assert r.status_code == 202
    job = client.get(f"/api/admin/rebalance/{r.json()['job_id']}").json()
    assert job["status"] == "COMPLETED"
    assert job["progress"]["DONE"] == 3

    for user, mult in [("a", 1), ("b", 3), ("c", 5)]:
        got = sorted((o["action"], o["ticker"], o["quantity"])
                     for o in orders_for(client, user_id=user, source="REBALANCE"))
        assert got == [("BUY", "IDEA", base * mult), ("SELL", "RELIANCE", base * mult)]
        held = {p["ticker"] for p in client.get(f"/api/subscriptions/{subs[user]}").json()["positions"]}
        assert "IDEA" in held and "RELIANCE" not in held


def test_newer_swap_waits_for_older_one(client):
    sub_id = subscribe(client, "user-101", 2)["subscription"]["id"]
    job1 = trigger_job(folio_id="folio-1", outgoing_ticker="RELIANCE", incoming_ticker="IDEA")
    job2 = trigger_job(folio_id="folio-1", outgoing_ticker="IDEA", incoming_ticker="WIPRO")

    async def run():
        async with SessionLocal() as db:
            from sqlalchemy import select

            t1 = await db.scalar(select(RebalanceTask.id).where(RebalanceTask.job_id == job1.id))
            t2 = await db.scalar(select(RebalanceTask.id).where(RebalanceTask.job_id == job2.id))
        processor = RebalanceProcessor(SessionLocal, MockBroker())
        first = await processor.process_task(t2)
        second = await processor.process_task(t1)
        third = await processor.process_task(t2)
        return first, second, third

    first, second, third = asyncio.run(run())
    assert (first, second, third) == (False, True, True)

    held = {p["ticker"] for p in client.get(f"/api/subscriptions/{sub_id}").json()["positions"]}
    assert "WIPRO" in held and "IDEA" not in held and "RELIANCE" not in held
    assert len(held) == 12


def test_dispatcher_republishes_unfinished_jobs(client, eager_celery):
    subscribe(client, "user-101", 2)
    job = trigger_job(folio_id="folio-1", outgoing_ticker="RELIANCE", incoming_ticker="IDEA")

    asyncio.run(CeleryDispatcher().start())

    assert client.get(f"/api/admin/rebalance/{job.id}").json()["status"] == "COMPLETED"
    assert len(orders_for(client, rebalance_job_id=job.id)) == 2


def _redis_up():
    try:
        import redis

        return redis.Redis.from_url(settings.redis_url, socket_connect_timeout=0.3).ping()
    except Exception:
        return False


needs_redis = pytest.mark.skipif(not _redis_up(), reason="Redis not running")


@needs_redis
def test_redis_lock_is_exclusive():
    lock = RedisKeyedLock(timeout=5, wait=5)

    async def run():
        inside, overlap = [0], [False]

        async def work():
            async with lock.hold("sub-test-exclusive"):
                inside[0] += 1
                if inside[0] > 1:
                    overlap[0] = True
                await asyncio.sleep(0.05)
                inside[0] -= 1

        await asyncio.gather(*(work() for _ in range(8)))
        return overlap[0]

    assert asyncio.run(run()) is False


@needs_redis
def test_redis_lock_gives_up_when_busy():
    holder = RedisKeyedLock(timeout=5, wait=1)
    waiter = RedisKeyedLock(timeout=5, wait=0.2)

    async def run():
        async with holder.hold("sub-test-busy"):
            with pytest.raises(ConflictError):
                async with waiter.hold("sub-test-busy"):
                    pass
        async with waiter.hold("sub-test-busy"):
            pass

    asyncio.run(run())


@needs_redis
def test_redis_cache_hit_and_invalidate():
    cache = RedisCache(ttl=30)
    calls = []

    async def loader():
        calls.append(1)
        return {"n": len(calls)}

    async def run():
        await cache.invalidate("test-ns")
        a = await cache.get_or_load("test-ns", ["k"], loader)
        b = await cache.get_or_load("test-ns", ["k"], loader)
        await cache.invalidate("test-ns")
        c = await cache.get_or_load("test-ns", ["k"], loader)
        return a, b, c

    a, b, c = asyncio.run(run())
    assert a == b == {"n": 1}
    assert c == {"n": 2}
    assert len(calls) == 2
