
import asyncio
import logging

from celery import Celery
from celery.signals import worker_process_init, worker_ready

from app.config import settings
from app.db.database import SessionLocal, engine
from app.services import rebalance_service
from app.services.broker_service import get_broker
from app.services.errors import ConflictError

log = logging.getLogger(__name__)

celery_app = Celery("folio", broker=settings.redis_url)
celery_app.conf.update(

    task_default_queue=settings.celery_queue,
    task_serializer="json",
    accept_content=["json"],
    task_ignore_result=True,          
    task_acks_late=True,             
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    broker_transport_options={"visibility_timeout": 300},
    broker_connection_retry_on_startup=True,
)


@worker_process_init.connect
def reset_db_pool(**_):

    engine.dispose(close=False)


@worker_ready.connect
def resume_unfinished_jobs(**_):

    try:
        job_ids = rebalance_service.pending_job_ids(SessionLocal)
    except Exception:
        log.exception("could not check for unfinished rebalance jobs")
        return
    for job_id in job_ids:
        log.info("worker start: re-publishing unfinished rebalance job %s", job_id)
        process_job.delay(job_id)


@celery_app.task(name="rebalance.process_job")
def process_job(job_id: str):
    task_ids = rebalance_service.start_job(SessionLocal, job_id)
    log.info("rebalance %s: fanning out %s subscribers", job_id, len(task_ids))
    if not task_ids:
        rebalance_service.finish_job(SessionLocal, job_id)
        return
    for task_id in task_ids:
        process_subscriber.delay(job_id, task_id)


@celery_app.task(name="rebalance.process_subscriber", bind=True, max_retries=300)
def process_subscriber(self, job_id: str, task_id: int):
    try:
        done = rebalance_service.process_task(SessionLocal, get_broker(), task_id)
    except ConflictError:

        raise self.retry(countdown=2)
    if not done:

        raise self.retry(countdown=1)
    rebalance_service.finish_job(SessionLocal, job_id)


class CeleryDispatcher:

    async def start(self):
     
        for job_id in await asyncio.to_thread(rebalance_service.pending_job_ids, SessionLocal):
            log.info("re-publishing unfinished rebalance job %s", job_id)
            self.enqueue(job_id)

    async def stop(self):
        pass

    def enqueue(self, job_id: str):
        process_job.delay(job_id)