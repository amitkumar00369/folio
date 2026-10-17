from typing import Protocol

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db
from app.services.broker_service import MockBroker, get_broker
from app.services.cache import get_cache
from app.services.folio_service import FolioService
from app.services.locks import subscription_locks
from app.services.order_service import OrderService
from app.services.rebalance_service import RebalanceService
from app.services.subscription_service import SubscriptionService

__all__ = [
    "JobQueue",
    "get_broker",
    "get_db",
    "get_folio_service",
    "get_order_service",
    "get_rebalance_service",
    "get_subscription_service",
    "get_worker",
]


class JobQueue(Protocol):
    async def enqueue(self, job_id: str): ...


def get_worker(request: Request):
    print("ererrerer request.app.state.rebalance_worker",request.app.state.rebalance_worker)
    return request.app.state.rebalance_worker


def get_folio_service(db: AsyncSession = Depends(get_db), cache=Depends(get_cache)):
    return FolioService(db, cache)


def get_order_service(db: AsyncSession = Depends(get_db)):
    return OrderService(db)


def get_subscription_service(
    db: AsyncSession = Depends(get_db),
    broker: MockBroker = Depends(get_broker),
    cache=Depends(get_cache),
):
    return SubscriptionService(db, broker, cache, subscription_locks)


def get_rebalance_service(db: AsyncSession = Depends(get_db), cache=Depends(get_cache)):
    return RebalanceService(db, cache)
