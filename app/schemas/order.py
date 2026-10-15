from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models import OrderAction, OrderSource, OrderStatus


class TradeRequest(BaseModel):
    user_id: str
    ticker: str
    action: OrderAction
    quantity: int = Field(gt=0)


class ExecutionReceipt(BaseModel):
    order_id: str
    status: OrderStatus
    timestamp: datetime
    user_id: str
    ticker: str
    action: OrderAction
    quantity: int


class OrderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    order_id: str
    user_id: str
    subscription_id: str | None
    ticker: str
    action: OrderAction
    quantity: int
    status: OrderStatus
    source: OrderSource
    rebalance_job_id: str | None
    timestamp: datetime


class OrderPage(BaseModel):
    items: list[OrderOut]
    count: int
    last_id: int | None
