
import threading
import time

import pytest

from app.config import settings
from app.db.database import SessionLocal
from app.models import RebalanceTask
from app.schemas.rebalance import RebalanceRequest
from app.services import rebalance_service
from app.services.broker_service import MockBroker
from app.services.errors import ConflictError
from app.services.locks import RedisKeyedLock
from app.workers.celery_app import CeleryDispatcher, celery_app
from tests.conftest import base_quantities, orders_for, subscribe


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
    """Celery can run tasks of two jobs in any order. A task must not run while
    an older task for the same subscriber is still pending."""
    sub_id = subscribe(client, "user-101", 2)["subscription"]["id"]
    with SessionLocal() as db:
        job1 = rebalance_service.trigger_rebalance(
            db, RebalanceRequest(folio_id="folio-1", outgoing_ticker="RELIANCE", incoming_ticker="IDEA"))
    with SessionLocal() as db:
        job2 = rebalance_service.trigger_rebalance(
            db, RebalanceRequest(folio_id="folio-1", outgoing_ticker="IDEA", incoming_ticker="WIPRO"))
    with SessionLocal() as db:
        t1 = db.query(RebalanceTask).filter_by(job_id=job1.id).one().id
        t2 = db.query(RebalanceTask).filter_by(job_id=job2.id).one().id

    broker = MockBroker()
    assert rebalance_service.process_task(SessionLocal, broker, t2) is False   # has to wait
    assert orders_for(client, rebalance_job_id=job2.id) == []

    assert rebalance_service.process_task(SessionLocal, broker, t1) is True
    assert rebalance_service.process_task(SessionLocal, broker, t2) is True

    held = {p["ticker"] for p in client.get(f"/api/subscriptions/{sub_id}").json()["positions"]}
    assert "WIPRO" in held and "IDEA" not in held and "RELIANCE" not in held
    assert len(held) == 12


def test_dispatcher_republishes_unfinished_jobs(client, eager_celery):
    subscribe(client, "user-101", 2)
    with SessionLocal() as db:   # committed but never published, like an API crash right after commit
        job = rebalance_service.trigger_rebalance(
            db, RebalanceRequest(folio_id="folio-1", outgoing_ticker="RELIANCE", incoming_ticker="IDEA"))

    import asyncio
    asyncio.run(CeleryDispatcher().start())

    assert client.get(f"/api/admin/rebalance/{job.id}").json()["status"] == "COMPLETED"
    assert len(orders_for(client, rebalance_job_id=job.id)) == 2


def _redis_up() -> bool:
    try:
        import redis

        return redis.Redis.from_url(settings.redis_url, socket_connect_timeout=0.3).ping()
    except Exception:
        return False


needs_redis = pytest.mark.skipif(not _redis_up(), reason="Redis not running")


@needs_redis
def test_redis_lock_is_exclusive():
    lock = RedisKeyedLock(settings.redis_url, timeout=5, wait=5)
    inside, overlap = [0], [False]

    def work():
        with lock.hold("sub-test-exclusive"):
            inside[0] += 1
            if inside[0] > 1:
                overlap[0] = True
            time.sleep(0.05)
            inside[0] -= 1

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert overlap[0] is False


@needs_redis
def test_redis_lock_gives_up_when_busy():
    holder = RedisKeyedLock(settings.redis_url, timeout=5, wait=1)
    waiter = RedisKeyedLock(settings.redis_url, timeout=5, wait=0.2)
    with holder.hold("sub-test-busy"):
        with pytest.raises(ConflictError):
            with waiter.hold("sub-test-busy"):
                pass
    with waiter.hold("sub-test-busy"): 
        pass
