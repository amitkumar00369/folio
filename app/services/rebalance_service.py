import logging
from collections import Counter

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import noload, selectinload

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
from app.services.broker_service import MockBroker
from app.services.cache import FOLIOS, SUBSCRIPTIONS, NoCache
from app.services.errors import DomainError, InvalidRequestError, NotFoundError
from app.services.folio_service import FolioService
from app.services.locks import subscription_locks
from app.services.order_service import OrderService
from app.util.consants import new_id, utcnow

log = logging.getLogger(__name__)

TERMINAL = (JobStatus.COMPLETED, JobStatus.COMPLETED_WITH_ERRORS)


async def task_counts(db: AsyncSession, job_id: str):
    rows = await db.scalars(select(RebalanceTask.status).where(RebalanceTask.job_id == job_id))
    counts = Counter({status: 0 for status in TaskStatus})
    counts.update(rows)
    return counts


class RebalanceService:
    def __init__(self, db: AsyncSession, cache=None):
        self.db = db
        self.cache = cache or NoCache()
        self.folios = FolioService(db)

    async def trigger(self, req: RebalanceRequest):
        folio = await self.folios.get_folio(req.folio_id)

        outgoing = folio.find_stock(req.outgoing_ticker)
        if outgoing is None:
            raise InvalidRequestError(f"{req.outgoing_ticker} is not part of folio '{folio.name}'")
        if req.incoming_ticker in folio.tickers:
            raise InvalidRequestError(f"{req.incoming_ticker} is already part of folio '{folio.name}'")

        incoming_qty = req.incoming_base_quantity or outgoing.base_quantity

        await self.folios.lock_folio(folio, bump=True)

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

        subscribers = (
            await self.db.execute(
                select(Subscription.id, Subscription.user_id, Subscription.multiplier).where(
                    Subscription.folio_id == folio.id, Subscription.active.is_(True)
                )
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

        self.db.add(job)
        await self.db.commit()
        await self.cache.invalidate(FOLIOS)

        log.info(
            "rebalance %s: %s %s -> %s, %s subscribers queued",
            job.id, folio.id, job.outgoing_ticker, job.incoming_ticker, job.total_tasks,
        )
        return job

    async def get_job(self, job_id: str, with_tasks: bool = False):
        options = []
        if with_tasks:
            options.append(
                selectinload(RebalanceJob.tasks).options(
                    noload(RebalanceTask.job), noload(RebalanceTask.subscription)
                )
            )
        job = await self.db.get(RebalanceJob, job_id, options=options)
        if job is None:
            raise NotFoundError(f"Rebalance job '{job_id}' not found")
        return job

    async def list_jobs(self, folio_id: str | None = None, limit: int = 50):
        stmt = select(RebalanceJob).order_by(RebalanceJob.created_at.desc()).limit(limit)
        if folio_id:
            stmt = stmt.where(RebalanceJob.folio_id == folio_id)
        return list(await self.db.scalars(stmt))

    async def retry_failed(self, job_id: str):
        job = await self.get_job(job_id)
        if job.status in (JobStatus.QUEUED, JobStatus.RUNNING):
            raise InvalidRequestError(f"job {job_id} is still {job.status}")
        result = await self.db.execute(
            update(RebalanceTask)
            .where(RebalanceTask.job_id == job_id, RebalanceTask.status == TaskStatus.FAILED)
            .values(status=TaskStatus.PENDING, error=None)
        )
        if result.rowcount:
            job.status = JobStatus.QUEUED
        await self.db.commit()
        return result.rowcount

    async def summary(self, job: RebalanceJob):
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
            "progress": dict(await task_counts(self.db, job.id)),
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "duration_ms": duration,
        }


class RebalanceProcessor:
    def __init__(self, session_factory: async_sessionmaker, broker: MockBroker, cache=None, locks=None):
        self.session_factory = session_factory
        self.broker = broker
        self.cache = cache or NoCache()
        self.locks = locks or subscription_locks

    async def pending_job_ids(self):
        async with self.session_factory() as db:
            return list(
                await db.scalars(
                    select(RebalanceJob.id)
                    .where(RebalanceJob.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]))
                    .order_by(RebalanceJob.created_at)
                )
            )

    async def start_job(self, job_id: str):
        async with self.session_factory() as db:
            job = await db.get(RebalanceJob, job_id)
            if job is None:
                log.warning("rebalance job %s vanished", job_id)
                return []
            job.status = JobStatus.RUNNING
            job.started_at = job.started_at or utcnow()
            job.finished_at = None
            task_ids = list(
                await db.scalars(
                    select(RebalanceTask.id)
                    .where(RebalanceTask.job_id == job_id, RebalanceTask.status == TaskStatus.PENDING)
                    .order_by(RebalanceTask.id)
                )
            )
            await db.commit()
            return task_ids

    async def process_task(self, task_id: int, wait_for_older: bool = True):
        async with self.session_factory() as db:
            subscription_id = await db.scalar(
                select(RebalanceTask.subscription_id).where(
                    RebalanceTask.id == task_id, RebalanceTask.status == TaskStatus.PENDING
                )
            )
            await db.commit()
        if subscription_id is None:
            return True

        async with self.locks.hold(subscription_id), self.session_factory() as db:
            task = await db.get(RebalanceTask, task_id)
            if task.status is not TaskStatus.PENDING:
                return True

            if wait_for_older and await self._has_older_pending(db, subscription_id, task_id):
                await db.rollback()
                return False

            job = task.job
            job_id = job.id
            sub = task.subscription
            task.attempts += 1

            if not sub.active:
                task.status = TaskStatus.SKIPPED
                task.error = "subscription exited before rebalance ran"
                await db.commit()
                return True

            try:
                await self._swap(db, job, sub)
            except DomainError as exc:
                await db.rollback()
                task = await db.get(RebalanceTask, task_id)
                task.status = TaskStatus.FAILED
                task.error = exc.message
                await db.commit()
                log.error("rebalance %s failed for %s: %s", job_id, subscription_id, exc.message)
                return True

            task.status = TaskStatus.DONE
            task.error = None
            await db.commit()

        await self.cache.invalidate(SUBSCRIPTIONS)
        return True

    async def _has_older_pending(self, db: AsyncSession, subscription_id: str, task_id: int):
        older = await db.scalar(
            select(RebalanceTask.id)
            .where(
                RebalanceTask.subscription_id == subscription_id,
                RebalanceTask.status == TaskStatus.PENDING,
                RebalanceTask.id < task_id,
            )
            .limit(1)
        )
        return older is not None

    async def _swap(self, db: AsyncSession, job: RebalanceJob, sub: Subscription):
        orders = OrderService(db, self.broker)
        current = set(await db.scalars(select(FolioStock.ticker).where(FolioStock.folio_id == job.folio_id)))

        held = sub.position_for(job.outgoing_ticker)
        if held and job.outgoing_ticker not in current:
            expected = job.outgoing_base_quantity * sub.multiplier
            if held.quantity != expected:
                log.warning(
                    "%s holds %s %s, expected %s; selling what is held",
                    sub.id, held.quantity, job.outgoing_ticker, expected,
                )
            await orders.execute_trade(
                sub, job.outgoing_ticker, OrderAction.SELL, held.quantity,
                OrderSource.REBALANCE, job.id,
            )
            await db.commit()

        if sub.position_for(job.incoming_ticker) is None and job.incoming_ticker in current:
            await orders.execute_trade(
                sub, job.incoming_ticker, OrderAction.BUY,
                job.incoming_base_quantity * sub.multiplier,
                OrderSource.REBALANCE, job.id,
            )
            await db.commit()

    async def finish_job(self, job_id: str):
        async with self.session_factory() as db:
            job = await db.get(RebalanceJob, job_id)
            print("console when we reflect rebalnce", )
            if job is None or job.status in TERMINAL:
                await db.commit()
                return False
            counts = await task_counts(db, job_id)
            print("counts", counts)
            if counts[TaskStatus.PENDING]:
                await db.commit()
                return False
            job.status = JobStatus.COMPLETED_WITH_ERRORS if counts[TaskStatus.FAILED] else JobStatus.COMPLETED
            job.finished_at = utcnow()
            await db.commit()
        log.info(
            "rebalance %s finished: %s",
            job_id, ", ".join(f"{k.value.lower()}={v}" for k, v in counts.items()),
        )
        return True
