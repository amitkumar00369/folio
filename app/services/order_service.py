from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Order, OrderAction, OrderSource, Position, Subscription
from app.schemas.order import TradeRequest
from app.services.broker_service import MockBroker
from app.services.errors import ConflictError


class OrderService:
    def __init__(self, db: AsyncSession, broker: MockBroker | None = None):
        self.db = db
        self.broker = broker

    async def execute_trade(
        self,
        sub: Subscription,
        ticker: str,
        action: OrderAction,
        quantity: int,
        source: OrderSource,
        rebalance_job_id: str | None = None,
    ):
        held = sub.position_for(ticker)
        if action is OrderAction.SELL and (held is None or held.quantity < quantity):
            have = held.quantity if held else 0
            raise ConflictError(f"cannot sell {quantity} {ticker}, subscription {sub.id} holds {have}")

        receipt = await self.broker.submit(
            TradeRequest(user_id=sub.user_id, ticker=ticker, action=action, quantity=quantity)
        )

        order = Order(
            order_id=receipt.order_id,
            user_id=sub.user_id,
            subscription=sub,
            ticker=ticker,
            action=action,
            quantity=quantity,
            status=receipt.status,
            source=source,
            rebalance_job_id=rebalance_job_id,
            timestamp=receipt.timestamp,
        )
        self.db.add(order)

        if action is OrderAction.BUY:
            if held:
                held.quantity += quantity
            else:
                sub.positions.append(Position(ticker=ticker, quantity=quantity))
        else:
            held.quantity -= quantity
            if held.quantity == 0:
                sub.positions.remove(held)

        return order

    async def list_orders(
        self,
        user_id: str | None = None,
        subscription_id: str | None = None,
        source: OrderSource | None = None,
        rebalance_job_id: str | None = None,
        after_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ):
        stmt = select(Order)
        if user_id:
            stmt = stmt.where(Order.user_id == user_id)
        if subscription_id:
            stmt = stmt.where(Order.subscription_id == subscription_id)
        if source:
            stmt = stmt.where(Order.source == source)
        if rebalance_job_id:
            stmt = stmt.where(Order.rebalance_job_id == rebalance_job_id)

        if after_id is not None:
            stmt = stmt.where(Order.id > after_id).order_by(Order.id)
        else:
            stmt = stmt.order_by(Order.id.desc())

        return list(await self.db.scalars(stmt.limit(limit).offset(offset)))
