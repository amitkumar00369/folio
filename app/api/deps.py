from typing import Protocol

from fastapi import Request

from app.db.database import get_db
from app.services.broker_service import get_broker

__all__ = ["get_broker", "get_db", "get_worker", "JobQueue"]


class JobQueue(Protocol):
    def enqueue(self, job_id: str) : ...


def get_worker(request: Request):
    return request.app.state.rebalance_worker
