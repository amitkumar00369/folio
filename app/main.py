import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api import admin, folios, orders, users
from app.config import settings
from app.db.database import SessionLocal, init_db
from app.db.seed import seed_folios
from app.services.broker_service import broker
from app.services.errors import DomainError
from app.workers.rebalance_worker import RebalanceWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    async with SessionLocal() as db:
        await seed_folios(db)

    if settings.use_celery:
        from app.workers.celery_app import CeleryDispatcher

        worker = CeleryDispatcher()
    else:
        worker = RebalanceWorker(SessionLocal, broker, settings.rebalance_concurrency)
    await worker.start()
    logging.getLogger(__name__).info("rebalance backend: %s", settings.queue_backend)
    app.state.rebalance_worker = worker
    try:
        yield
    finally:
        await worker.stop()


app = FastAPI(
    title="Folio Trading API",
    version="1.0.0",
    description="Basket trading with per-user multipliers and async auto-rebalancing.",
    lifespan=lifespan,
)

app.include_router(folios.router)
app.include_router(users.router)
app.include_router(orders.router)
app.include_router(admin.router)


@app.exception_handler(DomainError)
async def domain_error_handler(request: Request, exc: DomainError):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):

    errors = []
    for err in exc.errors():
        field = ".".join(str(p) for p in err["loc"] if p not in ("body", "query", "path"))
        msg = err["msg"].removeprefix("Value error, ")
        errors.append({"field": field, "message": msg})
    summary = "; ".join(f"{e['field']}: {e['message']}" if e["field"] else e["message"] for e in errors)
    return JSONResponse(status_code=400, content={"detail": summary, "errors": errors})


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
async def ui():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health", tags=["meta"])
async def health():
    return {"status": "ok", "queue_backend": settings.queue_backend}
