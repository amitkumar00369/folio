
import logging
from collections import Counter

from sqlalchemy import select, update
from sqlalchemy.orm import Session, sessionmaker

from app.models import (
    FolioStock,
    JobStatus,
    OrderAction,
    OrderSource,
    RebalanceJob,
    RebalanceTask,
    Subscription,
    TaskStatus,
)
from app.schemas.rebalance import RebalanceRequest
from app.services import folio_service
from app.services.broker_service import MockBroker
from app.services.errors import DomainError, InvalidRequestError, NotFoundError
from app.services.locks import subscription_locks
from app.services.order_service import execute_trade
from app.util.consants import new_id, utcnow

log = logging.getLogger(__name__)


def trigger_rebalance(db: Session, req: RebalanceRequest):
    folio = folio_service.get_folio(db, req.folio_id)

    outgoing = folio.find_stock(req.outgoing_ticker)
    print("outgoing", outgoing)
    if outgoing is None:
        raise InvalidRequestError(f"{req.outgoing_ticker} is not part of folio '{folio.name}'")
    if req.incoming_ticker in folio.tickers:
        raise InvalidRequestError(f"{req.incoming_ticker} is already part of folio '{folio.name}'")

    incoming_qty = req.incoming_base_quantity or outgoing.base_quantity

    folio_service.lock_folio(db, folio, bump=True)

    job = RebalanceJob(
        id=new_id("rb"),
        folio_id=folio.id,
        outgoing_ticker=outgoing.ticker,
        outgoing_base_quantity=outgoing.base_quantity,
        incoming_ticker=req.incoming_ticker,
        incoming_base_quantity=incoming_qty,
        status=JobStatus.QUEUED,
    )
    outgoing.ticker = req.incoming_ticker
    outgoing.base_quantity = incoming_qty

    subscribers = db.execute(
        select(Subscription.id, Subscription.user_id, Subscription.multiplier).where(
            Subscription.folio_id == folio.id, Subscription.active.is_(True)
        )
    ).all()
    job.tasks = [
        RebalanceTask(subscription_id=s.id, user_id=s.user_id, multiplier=s.multiplier)
        for s in subscribers
    ]
    job.total_tasks = len(subscribers)
    if not subscribers:
        job.status = JobStatus.COMPLETED
        job.started_at = job.finished_at = utcnow()

    db.add(job)
    db.commit()

    log.info(
        "rebalance %s: %s %s -> %s, %s subscribers queued",
        job.id, folio.id, job.outgoing_ticker, job.incoming_ticker, job.total_tasks,
    )
    return job


def start_job(session_factory: sessionmaker, job_id: str):
    with session_factory() as db:
        job = db.get(RebalanceJob, job_id)
        if job is None:
            log.warning("rebalance job %s vanished", job_id)
            return []
        job.status = JobStatus.RUNNING
        job.started_at = job.started_at or utcnow()
        job.finished_at = None
        task_ids = list(
            db.scalars(
                select(RebalanceTask.id)
                .where(RebalanceTask.job_id == job_id, RebalanceTask.status == TaskStatus.PENDING)
                .order_by(RebalanceTask.id)
            )
        )
        db.commit()
        return task_ids


def process_task(
    session_factory: sessionmaker,
    broker: MockBroker,
    task_id: int,
    wait_for_older: bool = True,
):

    with session_factory() as db:
        task = db.get(RebalanceTask, task_id)
        if task is None or task.status is not TaskStatus.PENDING:
            return True
        subscription_id = task.subscription_id

    with subscription_locks.hold(subscription_id), session_factory() as db:
        task = db.get(RebalanceTask, task_id)
        if task.status is not TaskStatus.PENDING:
            return True 

        if wait_for_older and _has_older_pending(db, subscription_id, task_id):
            db.rollback()
            return False

        job = task.job
        sub = task.subscription
        task.attempts += 1

        if not sub.active:
            task.status = TaskStatus.SKIPPED
            task.error = "subscription exited before rebalance ran"
            db.commit()
            return True

        try:
            _swap_for_subscriber(db, broker, job, sub)
        except DomainError as exc:
            db.rollback()
            task = db.get(RebalanceTask, task_id)
            task.status = TaskStatus.FAILED
            task.error = exc.message
            db.commit()
            log.error("rebalance %s failed for %s: %s", job.id, subscription_id, exc.message)
            return True

        task.status = TaskStatus.DONE
        task.error = None
        db.commit()
        return True


def _has_older_pending(db: Session, subscription_id: str, task_id: int):
    older = db.scalar(
        select(RebalanceTask.id)
        .where(
            RebalanceTask.subscription_id == subscription_id,
            RebalanceTask.status == TaskStatus.PENDING,
            RebalanceTask.id < task_id,
        )
        .limit(1)
    )
    return older is not None


def _swap_for_subscriber(db: Session, broker: MockBroker, job: RebalanceJob, sub: Subscription):
    current = set(db.scalars(select(FolioStock.ticker).where(FolioStock.folio_id == job.folio_id)))

    held = sub.position_for(job.outgoing_ticker)
    if held and job.outgoing_ticker not in current:
        expected = job.outgoing_base_quantity * sub.multiplier
        if held.quantity != expected:
            log.warning(
                "%s holds %s %s, expected %s; selling what is held",
                sub.id, held.quantity, job.outgoing_ticker, expected,
            )
        execute_trade(
            db, broker, sub, job.outgoing_ticker, OrderAction.SELL, held.quantity,
            OrderSource.REBALANCE, job.id,
        )
        db.commit()
    if sub.position_for(job.incoming_ticker) is None and job.incoming_ticker in current:
        execute_trade(
            db, broker, sub, job.incoming_ticker, OrderAction.BUY,
            job.incoming_base_quantity * sub.multiplier,
            OrderSource.REBALANCE, job.id,
        )
        db.commit()


def finish_job(session_factory: sessionmaker, job_id: str):
    """Close the job if no task is pending any more. Safe to call many times
    (the Celery path calls it after every subscriber)."""
    with session_factory() as db:
        job = db.get(RebalanceJob, job_id)
        if job is None or job.status in (JobStatus.COMPLETED, JobStatus.COMPLETED_WITH_ERRORS):
            return False
        counts = _task_counts(db, job_id)
        if counts[TaskStatus.PENDING]:
            return False
        job.status = JobStatus.COMPLETED_WITH_ERRORS if counts[TaskStatus.FAILED] else JobStatus.COMPLETED
        job.finished_at = utcnow()
        db.commit()
        log.info(
            "rebalance %s finished: %s",
            job_id, ", ".join(f"{k.value.lower()}={v}" for k, v in counts.items()),
        )
        return True


def retry_failed(db: Session, job_id: str):
    job = get_job(db, job_id)
    if job.status in (JobStatus.QUEUED, JobStatus.RUNNING):
        raise InvalidRequestError(f"job {job_id} is still {job.status}")
    reset = db.execute(
        update(RebalanceTask)
        .where(RebalanceTask.job_id == job_id, RebalanceTask.status == TaskStatus.FAILED)
        .values(status=TaskStatus.PENDING, error=None)
    ).rowcount
    if reset:
        job.status = JobStatus.QUEUED
    db.commit()
    return reset


def pending_job_ids(session_factory: sessionmaker):

    with session_factory() as db:
        return list(
            db.scalars(
                select(RebalanceJob.id)
                .where(RebalanceJob.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]))
                .order_by(RebalanceJob.created_at)
            )
        )


def get_job(db: Session, job_id: str):
    job = db.get(RebalanceJob, job_id)
    if job is None:
        raise NotFoundError(f"Rebalance job '{job_id}' not found")
    return job


def list_jobs(db: Session, folio_id: str | None = None, limit: int = 50):
    stmt = select(RebalanceJob).order_by(RebalanceJob.created_at.desc()).limit(limit)
    if folio_id:
        stmt = stmt.where(RebalanceJob.folio_id == folio_id)
    return list(db.scalars(stmt))


def _task_counts(db: Session, job_id: str):
    rows = db.execute(
        select(RebalanceTask.status).where(RebalanceTask.job_id == job_id)
    ).scalars()
    counts = Counter({status: 0 for status in TaskStatus})
    counts.update(rows)
    return counts


def job_summary(db: Session, job: RebalanceJob):
    duration = None
    if job.started_at and job.finished_at:
        duration = int((job.finished_at - job.started_at).total_seconds() * 1000)
    return {
        "id": job.id,
        "folio_id": job.folio_id,
        "outgoing_ticker": job.outgoing_ticker,
        "outgoing_base_quantity": job.outgoing_base_quantity,
        "incoming_ticker": job.incoming_ticker,
        "incoming_base_quantity": job.incoming_base_quantity,
        "status": job.status,
        "total_tasks": job.total_tasks,
        "progress": dict(_task_counts(db, job.id)),
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "duration_ms": duration,
    }
