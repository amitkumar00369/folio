from fastapi import APIRouter, Depends, Query, status

from app.api.deps import JobQueue, get_rebalance_service, get_subscription_service, get_worker
from app.schemas.rebalance import (
    RebalanceAccepted,
    RebalanceJobDetail,
    RebalanceJobOut,
    RebalanceRequest,
)
from app.schemas.subscription import UserSummary
from app.services.rebalance_service import RebalanceService
from app.services.subscription_service import SubscriptionService

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.post(
    "/rebalance",
    response_model=RebalanceAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Swap a stock in a folio and cascade it to all active subscribers",
)
async def trigger_rebalance(
    body: RebalanceRequest,
    service: RebalanceService = Depends(get_rebalance_service),
    worker: JobQueue = Depends(get_worker),
):
    job = await service.trigger(body)
    if job.total_tasks:
        await worker.enqueue(job.id)

    return RebalanceAccepted(
        job_id=job.id,
        folio_id=job.folio_id,
        outgoing_ticker=job.outgoing_ticker,
        incoming_ticker=job.incoming_ticker,
        incoming_base_quantity=job.incoming_base_quantity,
        subscribers_queued=job.total_tasks,
        status=job.status,
    )


@router.get("/rebalance", response_model=list[RebalanceJobOut])
async def list_rebalance_jobs(folio_id: str | None = None, service: RebalanceService = Depends(get_rebalance_service)):
    return [await service.summary(job) for job in await service.list_jobs(folio_id)]


@router.get("/rebalance/{job_id}", response_model=RebalanceJobDetail)
async def get_rebalance_job(job_id: str, service: RebalanceService = Depends(get_rebalance_service)):
    job = await service.get_job(job_id, with_tasks=True)
    return {**await service.summary(job), "tasks": job.tasks}


@router.post("/rebalance/{job_id}/retry", response_model=RebalanceJobOut)
async def retry_rebalance_job(
    job_id: str,
    service: RebalanceService = Depends(get_rebalance_service),
    worker: JobQueue = Depends(get_worker),
):
    if await service.retry_failed(job_id):
        await worker.enqueue(job_id)
    return await service.summary(await service.get_job(job_id))


@router.get("/users", response_model=list[UserSummary], summary="All users who have subscribed")
async def list_users(
    active: bool = Query(False, description="true = only users with at least one active subscription"),
    service: SubscriptionService = Depends(get_subscription_service),
):
    return await service.list_users(active_only=active)
