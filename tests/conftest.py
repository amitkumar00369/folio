import asyncio
import os
import tempfile
import time

_tmp = tempfile.mkdtemp(prefix="folio-tests-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["QUEUE_BACKEND"] = "inprocess"
os.environ["BROKER_LATENCY_MS"] = "0"
os.environ["BROKER_FAILURE_RATE"] = "0"
os.environ["BROKER_MAX_ATTEMPTS"] = "2"

import pytest  
from fastapi.testclient import TestClient  

from app.db.database import Base, SessionLocal, engine  
from app.main import app
from app.services.broker_service import MockBroker, get_broker  

TERMINAL = {"COMPLETED", "COMPLETED_WITH_ERRORS"}


async def drop_all():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
def client():
    asyncio.run(drop_all())
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def use_broker(client):
    def _use(broker: MockBroker):
        app.dependency_overrides[get_broker] = lambda: broker
        app.state.rebalance_worker.broker = broker
        return broker

    return _use


def subscribe(client, user_id: str, multiplier: int, folio_id: str = "folio-1"):
    r = client.post(
        "/api/subscriptions",
        json={"user_id": user_id, "folio_id": folio_id, "multiplier": multiplier},
    )
    assert r.status_code == 201, r.text
    return r.json()


def base_quantities(client, folio_id: str = "folio-1"):
    folio = client.get(f"/api/folios/{folio_id}").json()
    return {s["ticker"]: s["base_quantity"] for s in folio["stocks"]}


def wait_for_job(client, job_id: str, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = client.get(f"/api/admin/rebalance/{job_id}").json()
        if job["status"] in TERMINAL:
            return job
        time.sleep(0.02)
    raise AssertionError(f"rebalance job {job_id} did not finish in {timeout}s")


def orders_for(client, **params):
    return client.get("/api/orders", params={"limit": 1000, **params}).json()["items"]

def trigger_job(**swap):
    from app.schemas.rebalance import RebalanceRequest
    from app.services.rebalance_service import RebalanceService

    async def run():
        async with SessionLocal() as db:
            return await RebalanceService(db).trigger(RebalanceRequest(**swap))

    return asyncio.run(run())
