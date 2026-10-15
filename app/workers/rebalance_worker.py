import asyncio
import contextlib
import logging

from sqlalchemy.orm import sessionmaker

from app.services import rebalance_service
from app.services.broker_service import MockBroker

log = logging.getLogger(__name__)


class RebalanceWorker:


    def __init__(self, session_factory: sessionmaker, broker: MockBroker, concurrency: int = 4):
        self.session_factory = session_factory
        self.broker = broker
        self.concurrency = max(1, concurrency)
        self._queue: asyncio.Queue[str] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._runner: asyncio.Task | None = None

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue()
        self._runner = asyncio.create_task(self._run(), name="rebalance-worker")

        leftovers = await asyncio.to_thread(rebalance_service.pending_job_ids, self.session_factory)
        for job_id in leftovers:
            log.info("resuming unfinished rebalance job %s", job_id)
            self._queue.put_nowait(job_id)

    async def stop(self) -> None:
        if self._runner:
            self._runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._runner
            self._runner = None

    def enqueue(self, job_id: str) -> None:

        if self._loop is None or self._queue is None:
            raise RuntimeError("rebalance worker is not running")
        self._loop.call_soon_threadsafe(self._queue.put_nowait, job_id)

    async def _run(self) -> None:
        while True:
            job_id = await self._queue.get()
            try:
                await self.process_job(job_id)
            except Exception:

                log.exception("rebalance job %s crashed", job_id)
            finally:
                self._queue.task_done()

    async def process_job(self, job_id: str) -> None:
        task_ids = await asyncio.to_thread(rebalance_service.start_job, self.session_factory, job_id)
        log.info("rebalance %s: processing %s subscribers", job_id, len(task_ids))

        sem = asyncio.Semaphore(self.concurrency)

        async def run_one(task_id: int) -> None:
            async with sem:
                await asyncio.to_thread(
                    rebalance_service.process_task,
                    self.session_factory, self.broker, task_id, False,
                )

        results = await asyncio.gather(*(run_one(t) for t in task_ids), return_exceptions=True)
        for task_id, res in zip(task_ids, results):
            if isinstance(res, Exception):
                log.error("rebalance task %s raised unexpectedly: %r", task_id, res)

        await asyncio.to_thread(rebalance_service.finish_job, self.session_factory, job_id)
