import asyncio

from app.services.broker_service import MockBroker
from app.services.errors import BrokerError
from tests.conftest import base_quantities, orders_for, subscribe, wait_for_job, trigger_job

SWAP = {"folio_id": "folio-1", "outgoing_ticker": "RELIANCE", "incoming_ticker": "IDEA"}


def positions(client, sub_id: str):
    sub = client.get(f"/api/subscriptions/{sub_id}").json()
    return {p["ticker"]: p["quantity"] for p in sub["positions"]}


def test_swap_updates_folio(client):
    r = client.post("/api/admin/rebalance", json={**SWAP, "incoming_base_quantity": 4})
    assert r.status_code == 202

    stocks = client.get("/api/folios/folio-1").json()["stocks"]
    tickers = [s["ticker"] for s in stocks]
    assert len(stocks) == 12
    assert "RELIANCE" not in tickers
    assert tickers[0] == "IDEA"  
    assert stocks[0]["base_quantity"] == 4


def test_cascade_respects_each_users_multiplier(client):
    reliance_base = base_quantities(client)["RELIANCE"]
    subs = {
        user: subscribe(client, user, mult)["subscription"]["id"]
        for user, mult in [("user-a", 1), ("user-b", 3), ("user-c", 5)]
    }

    r = client.post("/api/admin/rebalance", json=SWAP)
    assert r.status_code == 202
    assert r.json()["subscribers_queued"] == 3

    job = wait_for_job(client, r.json()["job_id"])
    assert job["status"] == "COMPLETED"
    assert job["progress"]["DONE"] == 3

    for user, mult in [("user-a", 1), ("user-b", 3), ("user-c", 5)]:
        rb = orders_for(client, user_id=user, source="REBALANCE")
        got = sorted((o["action"], o["ticker"], o["quantity"]) for o in rb)
        assert got == [
            ("BUY", "IDEA", reliance_base * mult),
            ("SELL", "RELIANCE", reliance_base * mult),
        ]
        held = positions(client, subs[user])
        assert "RELIANCE" not in held
        assert held["IDEA"] == reliance_base * mult
        assert len(held) == 12


def test_incoming_base_quantity_is_used_for_buy(client):
    subscribe(client, "user-101", 3)
    job_id = client.post("/api/admin/rebalance", json={**SWAP, "incoming_base_quantity": 7}).json()["job_id"]
    wait_for_job(client, job_id)

    buy = [o for o in orders_for(client, rebalance_job_id=job_id) if o["action"] == "BUY"]
    assert len(buy) == 1 and buy[0]["quantity"] == 21


def test_inactive_subscribers_are_left_out(client):
    gone = subscribe(client, "user-gone", 2)["subscription"]["id"]
    client.post(f"/api/subscriptions/{gone}/exit")
    subscribe(client, "user-stays", 2)
    subscribe(client, "other-folio", 2, folio_id="folio-2") 

    r = client.post("/api/admin/rebalance", json=SWAP)
    assert r.json()["subscribers_queued"] == 1
    wait_for_job(client, r.json()["job_id"])

    assert orders_for(client, user_id="user-gone", source="REBALANCE") == []
    assert orders_for(client, user_id="other-folio", source="REBALANCE") == []
    assert len(orders_for(client, user_id="user-stays", source="REBALANCE")) == 2


def test_no_subscribers_completes_immediately(client):
    r = client.post("/api/admin/rebalance", json=SWAP)
    assert r.status_code == 202
    assert r.json()["subscribers_queued"] == 0
    assert r.json()["status"] == "COMPLETED"


def test_admin_call_does_not_wait_for_cascade(client, use_broker):
    for i in range(20):
        subscribe(client, f"user-{i}", (i % 5) + 1)
    use_broker(MockBroker(latency_ms=30))  

    r = client.post("/api/admin/rebalance", json=SWAP)
    assert r.status_code == 202
    body = r.json()
    assert body["subscribers_queued"] == 20

    
    snapshot = client.get(f"/api/admin/rebalance/{body['job_id']}").json()
    assert snapshot["status"] in ("QUEUED", "RUNNING")
    assert snapshot["progress"]["PENDING"] > 0

    job = wait_for_job(client, body["job_id"])
    assert job["progress"]["DONE"] == 20
    assert len(orders_for(client, rebalance_job_id=body["job_id"])) == 40


def test_many_subscribers_all_rebalanced(client):
    base = base_quantities(client)["RELIANCE"]
    mults = {f"u{i}": (i % 7) + 1 for i in range(60)}
    for user, m in mults.items():
        subscribe(client, user, m)

    job_id = client.post("/api/admin/rebalance", json=SWAP).json()["job_id"]
    job = wait_for_job(client, job_id, timeout=30)
    assert job["progress"]["DONE"] == 60

    orders = orders_for(client, rebalance_job_id=job_id)
    assert len(orders) == 120
    for o in orders:
        assert o["quantity"] == base * mults[o["user_id"]]


def test_exit_after_rebalance_sells_new_stock(client):
    sub_id = subscribe(client, "user-101", 3)["subscription"]["id"]
    wait_for_job(client, client.post("/api/admin/rebalance", json=SWAP).json()["job_id"])

    sells = client.post(f"/api/subscriptions/{sub_id}/exit").json()["orders"]
    tickers = {o["ticker"] for o in sells}
    assert len(sells) == 12
    assert "IDEA" in tickers and "RELIANCE" not in tickers


def test_new_subscriber_after_swap_buys_new_basket(client):
    wait_for_job(client, client.post("/api/admin/rebalance", json=SWAP).json()["job_id"])
    orders = subscribe(client, "late-user", 2)["orders"]
    assert "IDEA" in {o["ticker"] for o in orders}
    assert "RELIANCE" not in {o["ticker"] for o in orders}


def test_exit_before_cascade_runs_is_skipped(client):
    sub_id = subscribe(client, "user-101", 3)["subscription"]["id"]

    job = trigger_job(**SWAP)

    sells = client.post(f"/api/subscriptions/{sub_id}/exit").json()["orders"]
    assert "RELIANCE" in {o["ticker"] for o in sells}

    asyncio.run(client.app.state.rebalance_worker.process_job(job.id))

    detail = client.get(f"/api/admin/rebalance/{job.id}").json()
    assert detail["tasks"][0]["status"] == "SKIPPED"
    assert orders_for(client, rebalance_job_id=job.id) == []
    assert positions(client, sub_id) == {}


class FailOnTicker(MockBroker):
    def __init__(self, ticker: str):
        super().__init__()
        self.ticker = ticker
        self.enabled = True

    async def place_order(self, trade):
        if self.enabled and trade.ticker == self.ticker:
            raise BrokerError(f"no liquidity in {trade.ticker}")
        return await super().place_order(trade)


def test_failed_buy_is_recorded_and_retry_finishes_it(client, use_broker):
    base = base_quantities(client)["RELIANCE"]
    ok = subscribe(client, "user-101", 3)["subscription"]["id"]
    broker = use_broker(FailOnTicker("IDEA"))

    job_id = client.post("/api/admin/rebalance", json=SWAP).json()["job_id"]
    job = wait_for_job(client, job_id)
    assert job["status"] == "COMPLETED_WITH_ERRORS"
    assert job["progress"]["FAILED"] == 1


    rb = orders_for(client, rebalance_job_id=job_id)
    assert [(o["action"], o["ticker"]) for o in rb] == [("SELL", "RELIANCE")]
    held = positions(client, ok)
    assert "RELIANCE" not in held and "IDEA" not in held

    broker.enabled = False
    assert client.post(f"/api/admin/rebalance/{job_id}/retry").status_code == 200
    job = wait_for_job(client, job_id)
    assert job["status"] == "COMPLETED"


    rb = orders_for(client, rebalance_job_id=job_id)
    assert sorted((o["action"], o["ticker"]) for o in rb) == [("BUY", "IDEA"), ("SELL", "RELIANCE")]
    assert positions(client, ok)["IDEA"] == base * 3


def test_two_swaps_in_a_row(client):
    base = base_quantities(client)
    sub_id = subscribe(client, "user-101", 2)["subscription"]["id"]

    j1 = client.post("/api/admin/rebalance", json=SWAP).json()["job_id"]
    j2 = client.post(
        "/api/admin/rebalance",
        json={"folio_id": "folio-1", "outgoing_ticker": "TCS", "incoming_ticker": "WIPRO", "incoming_base_quantity": 3},
    ).json()["job_id"]
    wait_for_job(client, j1)
    wait_for_job(client, j2)

    held = positions(client, sub_id)
    assert held["IDEA"] == base["RELIANCE"] * 2
    assert held["WIPRO"] == 6
    assert "RELIANCE" not in held and "TCS" not in held
    assert len(held) == 12
