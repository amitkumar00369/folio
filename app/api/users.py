from fastapi import APIRouter, Depends, Query, status

from app.api.deps import get_subscription_service
from app.schemas.subscription import SubscribeRequest, SubscriptionOut, SubscriptionWithOrders
from app.services.subscription_service import SubscriptionService

router = APIRouter(prefix="/api", tags=["subscriptions"])


@router.post(
    "/subscriptions",
    response_model=SubscriptionWithOrders,
    status_code=status.HTTP_201_CREATED,
    summary="Subscribe a user to a folio (places 12 BUY orders)",
)
async def subscribe(body: SubscribeRequest, service: SubscriptionService = Depends(get_subscription_service)):
    sub, orders = await service.subscribe(body.user_id, body.folio_id, body.multiplier)
    return {"subscription": sub, "orders": orders}


@router.get("/subscriptions", response_model=list[SubscriptionOut], summary="All subscriptions (all users)")
async def list_subscriptions(
    folio_id: str | None = None,
    active: bool | None = Query(None, description="true = only active, false = only exited"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    service: SubscriptionService = Depends(get_subscription_service),
):
    return await service.list_all(folio_id, active, limit, offset)


@router.get("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
async def get_subscription(subscription_id: str, service: SubscriptionService = Depends(get_subscription_service)):
    return await service.get(subscription_id)


@router.post(
    "/subscriptions/{subscription_id}/exit",
    response_model=SubscriptionWithOrders,
    summary="Exit a folio (sells every position)",
)
async def exit_subscription(subscription_id: str, service: SubscriptionService = Depends(get_subscription_service)):
    sub, orders = await service.exit(subscription_id)
    return {"subscription": sub, "orders": orders}


@router.get("/users/{user_id}/subscriptions", response_model=list[SubscriptionOut])
async def list_user_subscriptions(
    user_id: str,
    active: bool | None = Query(None, description="filter on active / exited"),
    service: SubscriptionService = Depends(get_subscription_service),
):
    return await service.list_for_user(user_id, active)
