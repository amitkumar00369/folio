from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from app.api.deps import JobQueue, get_db, get_worker
from app.schemas.rebalance import (
    RebalanceAccepted,
    RebalanceJobDetail,
    RebalanceJobOut,
    RebalanceRequest,
)
from app.services import rebalance_service

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.post(
    "/rebalance",
    response_model=RebalanceAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Swap a stock in a folio and cascade it to all active subscribers",
)
def trigger_rebalance(
    body: RebalanceRequest,
    db: Session = Depends(get_db),
    worker: JobQueue = Depends(get_worker),
):
    job = rebalance_service.trigger_rebalance(db, body)
    if job.total_tasks:
        worker.enqueue(job.id)

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
def list_rebalance_jobs(folio_id: str | None = None, db: Session = Depends(get_db)):
    return [rebalance_service.job_summary(db, j) for j in rebalance_service.list_jobs(db, folio_id)]


@router.get("/rebalance/{job_id}", response_model=RebalanceJobDetail)
def get_rebalance_job(job_id: str, db: Session = Depends(get_db)):
    job = rebalance_service.get_job(db, job_id)
    return {**rebalance_service.job_summary(db, job), "tasks": job.tasks}


@router.post("/rebalance/{job_id}/retry", response_model=RebalanceJobOut)
def retry_rebalance_job(
    job_id: str,
    db: Session = Depends(get_db),
    worker: JobQueue = Depends(get_worker),
):

    if rebalance_service.retry_failed(db, job_id):
        worker.enqueue(job_id)
    return rebalance_service.job_summary(db, rebalance_service.get_job(db, job_id))
