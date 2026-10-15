import asyncio

import pytest
from pydantic import ValidationError

from app.models import OrderAction, OrderStatus
from app.schemas.order import TradeRequest
from app.services.broker_service import MockBroker
from app.services.errors import BrokerError


def trade(**overrides):
    data = {"user_id": "user-101", "ticker": "RELIANCE", "action": "BUY", "quantity": 6}
    return TradeRequest(**{**data, **overrides})


def test_receipt_carries_trade_details():
    receipt = asyncio.run(MockBroker().place_order(trade(action="SELL", quantity=10)))

    assert receipt.order_id.startswith("ord-")
    assert receipt.status is OrderStatus.EXECUTED
    assert receipt.timestamp.tzinfo is not None
    assert (receipt.user_id, receipt.ticker, receipt.action, receipt.quantity) == (
        "user-101", "RELIANCE", OrderAction.SELL, 10,
    )


def test_order_ids_are_unique():
    broker = MockBroker()
    ids = {asyncio.run(broker.place_order(trade())).order_id for _ in range(500)}
    assert len(ids) == 500


@pytest.mark.parametrize("qty", [0, -5])
def test_non_positive_quantity_rejected(qty):
    with pytest.raises(ValidationError):
        trade(quantity=qty)


def test_bad_action_rejected():
    with pytest.raises(ValidationError):
        trade(action="HOLD")


def test_failure_rate_raises():
    with pytest.raises(BrokerError):
        asyncio.run(MockBroker(failure_rate=1.0).place_order(trade()))


class Flaky(MockBroker):
    def __init__(self, fail_times: int):
        super().__init__()
        self.fail_times = fail_times
        self.calls = 0

    async def place_order(self, t):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise BrokerError("timeout")
        return await super().place_order(t)


def test_retry_recovers_from_transient_failure():
    broker = Flaky(fail_times=1)
    assert asyncio.run(broker.submit(trade())).status is OrderStatus.EXECUTED
    assert broker.calls == 2


def test_retry_gives_up(client, use_broker):
    use_broker(MockBroker(failure_rate=1.0))
    r = client.post("/api/subscriptions", json={"user_id": "u1", "folio_id": "folio-1", "multiplier": 1})
    assert r.status_code == 502
    assert client.get("/api/users/u1/subscriptions").json() == []
    assert client.get("/api/orders").json()["count"] == 0
