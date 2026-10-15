from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_broker, get_db
from app.schemas.subscription import SubscribeRequest, SubscriptionOut, SubscriptionWithOrders
from app.services import subscription_service
from app.services.broker_service import MockBroker

router = APIRouter(prefix="/api", tags=["subscriptions"])


@router.post(
    "/subscriptions",
    response_model=SubscriptionWithOrders,
    status_code=status.HTTP_201_CREATED,
    summary="Subscribe a user to a folio (places 12 BUY orders)",
)
def subscribe(
    body: SubscribeRequest,
    db: Session = Depends(get_db),
    broker: MockBroker = Depends(get_broker),
):
    sub, orders = subscription_service.subscribe(db, broker, body.user_id, body.folio_id, body.multiplier)
    return {"subscription": sub, "orders": orders}


@router.get("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
def get_subscription(subscription_id: str, db: Session = Depends(get_db)):
    return subscription_service.get_subscription(db, subscription_id)


@router.post(
    "/subscriptions/{subscription_id}/exit",
    response_model=SubscriptionWithOrders,
    summary="Exit a folio (sells every position)",
)
def exit_subscription(
    subscription_id: str,
    db: Session = Depends(get_db),
    broker: MockBroker = Depends(get_broker),
):
    sub, orders = subscription_service.exit_subscription(db, broker, subscription_id)
    return {"subscription": sub, "orders": orders}


@router.get("/users/{user_id}/subscriptions", response_model=list[SubscriptionOut])
def list_user_subscriptions(
    user_id: str,
    active: bool | None = Query(None, description="filter on active / exited"),
    db: Session = Depends(get_db),
):
    return subscription_service.list_user_subscriptions(db, user_id, active)

@router.get("/subscriptions", response_model=list[SubscriptionOut], summary="All subscriptions (all users)")
def list_subscriptions(
    folio_id: str | None = None,
    active: bool | None = Query(None, description="true = only active, false = only exited"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    return subscription_service.list_subscriptions(db, folio_id, active, limit, offset)