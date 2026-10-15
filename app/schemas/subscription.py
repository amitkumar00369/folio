from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.config import settings
from app.schemas.order import OrderOut


class SubscribeRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=64, examples=["user-101"])
    folio_id: str = Field(min_length=1, examples=["folio-1"])
    multiplier: int = Field(ge=1, le=settings.max_multiplier, examples=[3])

    @field_validator("user_id", "folio_id")
    @classmethod
    def strip_and_require(cls, v: str):
        v = v.strip()
        if not v:
            raise ValueError("must not be blank")
        return v


class PositionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker: str
    quantity: int


class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    folio_id: str
    folio_name: str
    multiplier: int
    active: bool
    created_at: datetime
    exited_at: datetime | None
    positions: list[PositionOut]


class SubscriptionWithOrders(BaseModel):
    subscription: SubscriptionOut
    orders: list[OrderOut]



class UserSummary(BaseModel):
    user_id: str
    total_subscriptions: int
    active_subscriptions: int
    active_folios: list[str]
    first_subscribed_at: datetime
