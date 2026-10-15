import logging
import random
import time

from app.config import settings
from app.models import OrderStatus
from app.schemas.order import ExecutionReceipt, TradeRequest
from app.services.errors import BrokerError
from app.util.consants import new_id, utcnow

log = logging.getLogger(__name__)


class MockBroker:


    def __init__(self, latency_ms: int = 0, failure_rate: float = 0.0):
        self.latency_ms = latency_ms
        self.failure_rate = failure_rate

    def place_order(self, trade: TradeRequest):
        if self.latency_ms:
            time.sleep(self.latency_ms / 1000)

        if self.failure_rate and random.random() < self.failure_rate:
            raise BrokerError(f"broker rejected {trade.action} {trade.ticker} x{trade.quantity}")

        return ExecutionReceipt(
            order_id=new_id("ord"),
            status=OrderStatus.EXECUTED,
            timestamp=utcnow(),
            user_id=trade.user_id,
            ticker=trade.ticker,
            action=trade.action,
            quantity=trade.quantity,
        )


def submit_with_retry(broker: MockBroker, trade: TradeRequest):

    attempts = max(1, settings.broker_max_attempts)
    for attempt in range(1, attempts + 1):
        try:
            return broker.place_order(trade)
        except BrokerError as exc:
            if attempt == attempts:
                raise
            log.warning("broker call failed (attempt %s/%s): %s", attempt, attempts, exc.message)
            time.sleep(0.05 * 2 ** (attempt - 1))
    raise AssertionError("unreachable")


broker = MockBroker(settings.broker_latency_ms, settings.broker_failure_rate)


def get_broker():
    return broker
