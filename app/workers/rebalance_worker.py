import asyncio
import contextlib
import logging

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.services.broker_service import MockBroker
from app.services.cache import get_cache
from app.services.locks import subscription_locks
from app.services.rebalance_service import RebalanceProcessor

log = logging.getLogger(__name__)


class RebalanceWorker:
    def __init__(self, session_factory: async_sessionmaker, broker: MockBroker, concurrency: int = 4):
        self.session_factory = session_factory
        self.broker = broker
        self.concurrency = max(1, concurrency)
        self._queue: asyncio.Queue[str] | None = None
        self._runner: asyncio.Task | None = None

    def processor(self):
        return RebalanceProcessor(self.session_factory, self.broker, get_cache(), subscription_locks)

    async def start(self):
        self._queue = asyncio.Queue()
        self._runner = asyncio.create_task(self._run(), name="rebalance-worker")
        for job_id in await self.processor().pending_job_ids():
            log.info("resuming unfinished rebalance job %s", job_id)
            self._queue.put_nowait(job_id)

    async def stop(self):
        if self._runner:
            self._runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._runner
            self._runner = None

    async def enqueue(self, job_id: str):
        if self._queue is None:
            raise RuntimeError("rebalance worker is not running")
        self._queue.put_nowait(job_id)

    async def _run(self):
        while True:
            job_id = await self._queue.get()
            try:
                await self.process_job(job_id)
            except Exception:
                log.exception("rebalance job %s crashed", job_id)
            finally:
                self._queue.task_done()

    async def process_job(self, job_id: str):
        processor = self.processor()
        task_ids = await processor.start_job(job_id)
        log.info("rebalance %s: processing %s subscribers", job_id, len(task_ids))

        sem = asyncio.Semaphore(self.concurrency)

        async def run_one(task_id: int):
            async with sem:
                await processor.process_task(task_id, wait_for_older=False)

        results = await asyncio.gather(*(run_one(t) for t in task_ids), return_exceptions=True)
        for task_id, res in zip(task_ids, results):
            if isinstance(res, Exception):
                log.error("rebalance task %s raised unexpectedly: %r", task_id, res)

        await processor.finish_job(job_id)
