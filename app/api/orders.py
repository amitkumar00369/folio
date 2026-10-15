from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models import OrderSource
from app.schemas.order import OrderPage
from app.services import order_service

router = APIRouter(prefix="/api/orders", tags=["orders"])


@router.get("", response_model=OrderPage)
def list_orders(
    user_id: str | None = None,
    subscription_id: str | None = None,
    source: OrderSource | None = None,
    rebalance_job_id: str | None = None,
    after_id: int | None = Query(None, ge=0, description="only orders newer than this id (oldest first)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    orders = order_service.list_orders(
        db,
        user_id=user_id,
        subscription_id=subscription_id,
        source=source,
        rebalance_job_id=rebalance_job_id,
        after_id=after_id,
        limit=limit,
        offset=offset,
    )
    last_id = max((o.id for o in orders), default=after_id)
    return OrderPage(items=orders, count=len(orders), last_id=last_id)
