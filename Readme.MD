# Folio Management & Auto-Rebalancing

A basket-trading prototype. Users subscribe to a **folio** (a basket of exactly 12 stocks) with their own quantity multiplier, can exit at any time, and an admin can swap a stock in a folio. The swap then cascades to every active subscriber in the background.

**Stack:** Python, FastAPI (async), SQLAlchemy 2.0 async + aiosqlite (SQLite), Pydantic v2, Celery + Redis, pytest, plain HTML/JS UI. The broker is a mock.

The core rule everywhere:

```
user order quantity = folio base quantity × user's own multiplier
```

---

## 1. Setup

Requires Python 3.11+. Redis is only needed for Celery mode.

```bash
python -m venv fastenv
.fastenv\Scripts\activate            # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
copy .env.example .env            # Linux/macOS: cp .env.example .env
```

### `.env`

| Variable | Default | Meaning |
|---|---|---|
| `QUEUE_BACKEND` | `inprocess` | `inprocess` (asyncio worker inside the API) or `celery` |
| `DATABASE_URL` | `sqlite+aiosqlite:///./folio.db` | Database |
| `REDIS_URL` | `redis://localhost:6379/1` | Celery broker, locks and cache (use `127.0.0.1` on Windows) |
| `CELERY_QUEUE` | `folio` | Dedicated queue name |
| `CACHE_TTL_SECONDS` | `60` | Redis cache TTL for list endpoints (Celery mode) |
| `REBALANCE_CONCURRENCY` | `4` | Parallel subscribers per job (in-process mode) |
| `BROKER_LATENCY_MS` / `BROKER_FAILURE_RATE` / `BROKER_MAX_ATTEMPTS` | `0` / `0` / `3` | Mock broker behaviour |

## 2. How to run

### Option A: no Redis (default, in-process worker)

```bash
uvicorn app.main:app --reload
```

### Option B: Celery + Redis (production-style)

Set `QUEUE_BACKEND=celery` in `.env`, start Redis, then:

```bash
# terminal 1
uvicorn app.main:app --reload

# terminal 2 (Windows needs the threads pool)
celery -A app.workers.celery_app worker --pool=threads --concurrency=4 --loglevel=info
```

Then open:

- UI: http://localhost:8000/
- Swagger: http://localhost:8000/docs
- Health: http://localhost:8000/health (shows the active queue backend)

On first start the tables are created and the 7 folios are seeded (Alpha Growth, Bluechip Core, India Momentum, Dividend Leaders, Tech Innovators, Consumer Power, Value Select, 12 stocks each). Seeding skips folios that already exist, so restarts don't undo admin swaps. Delete `folio.db` to start clean.

## 3. API overview

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/folios` | List folios with their 12 stocks |
| GET | `/api/folios/{folio_id}` | Folio details |
| POST | `/api/subscriptions` | Subscribe (12 BUY orders) → `201` |
| GET | `/api/subscriptions` | All subscriptions (`folio_id`, `active`, `limit`, `offset`) |
| GET | `/api/subscriptions/{id}` | One subscription with current positions |
| POST | `/api/subscriptions/{id}/exit` | Exit (12 SELL orders) |
| GET | `/api/users/{user_id}/subscriptions` | A user's subscriptions (`?active=true/false`) |
| GET | `/api/orders` | Execution receipts (`user_id`, `subscription_id`, `source`, `rebalance_job_id`, `after_id`, `limit`, `offset`) |
| POST | `/api/admin/rebalance` | Swap a stock → `202` immediately, cascade in background |
| GET | `/api/admin/rebalance` | Rebalance jobs with progress |
| GET | `/api/admin/rebalance/{job_id}` | Job status + per-subscriber tasks |
| POST | `/api/admin/rebalance/{job_id}/retry` | Re-run failed subscriber tasks |
| GET | `/api/admin/users` | Users with subscription counts |
| GET | `/docs` | Swagger UI |

Errors: `404` not found, `400` validation / bad ticker / invalid quantity, `409` conflicts (already subscribed, already exited, concurrent folio edit), `502` broker failed after retries. Body: `{"detail": "..."}`.

## 4. Architecture

```
Browser UI / Swagger
        │
FastAPI routes (app/api)  ──Depends──►  service classes (app/services)
        │                                   │            │            │
        │                              SQLite (DB)   Redis (cache,  Mock broker
        │                                             lock, queue)
        └── enqueue(job_id) ──► worker: asyncio (in-process) or Celery
                                       └── RebalanceProcessor ─► OrderService ─► broker
```

```
app/
  main.py          app, lifespan (create tables, seed, start worker), error handlers, UI route
  config.py        settings from .env
  api/             routes + deps.py (dependency injection)
  services/        FolioService, SubscriptionService, OrderService, RebalanceService,
                   RebalanceProcessor, MockBroker, cache, locks, errors
  models/          SQLAlchemy tables
  schemas/         Pydantic request/response models
  workers/         rebalance_worker.py (asyncio), celery_app.py (Celery tasks)
  db/              engine/session, seed, UTC datetime type
static/index.html  UI
tests/             pytest
```

- Routes are thin; business logic lives in service classes, so the API and the Celery worker share the same code.
- Services raise domain errors (`NotFoundError`, `ConflictError`, ...) and one handler maps them to HTTP status codes.

### Data model

| Table | Purpose |
|---|---|
| `folios` | id, name, `version` (optimistic lock) |
| `folio_stocks` | 12 rows per folio: slot, ticker, base_quantity |
| `subscriptions` | user_id, folio_id, **multiplier**, active, exited_at (exit = soft delete) |
| `positions` | what each subscription currently holds |
| `orders` | every BUY/SELL execution receipt (append-only audit log) |
| `rebalance_jobs` | one per admin swap: outgoing/incoming ticker + base qty, status |
| `rebalance_tasks` | one per active subscriber: PENDING / DONE / SKIPPED / FAILED |

A partial unique index allows only one **active** subscription per user and folio.

## 5. Async / rebalancing approach

1. `POST /api/admin/rebalance` validates the request. The outgoing ticker must be in the folio, the incoming ticker must not be, and the incoming base quantity must be positive.
2. In **one transaction**, it bumps the folio version, swaps the slot's ticker and base quantity, creates a `RebalanceJob`, and creates one `RebalanceTask` per **active** subscriber (inactive users are excluded).
3. After the commit it enqueues the `job_id` and returns **202** with `subscribers_queued`. It does not wait for any broker order.
4. A background worker picks the job up:
   - **in-process:** an asyncio queue + background task in the API process, with a semaphore for concurrency;
   - **Celery:** `process_job` fans out one `process_subscriber` task per subscriber through Redis.
5. For each subscriber, `RebalanceProcessor.process_task` takes a per-subscription lock, then:
   - SELLs the outgoing stock (the held quantity, i.e. outgoing base × that user's multiplier);
   - BUYs the incoming stock (incoming base × that user's multiplier);
   - commits each leg and marks the task DONE.
6. When no task is PENDING, the job becomes COMPLETED (or COMPLETED_WITH_ERRORS).

Reliability:

- **Idempotent:** only PENDING tasks run, so duplicate deliveries never double-trade.
- **Crash-safe:** jobs and tasks live in the DB. Unfinished jobs are resumed on API start and on Celery `worker_ready`. Celery uses `acks_late`.
- **Concurrency:**
  - an exit and a cascade for the same subscriber never overlap (per-subscription lock: asyncio, or Redis in Celery mode);
  - subscribe vs. swap races are handled by the folio version check;
  - a subscriber's swaps apply in order.
- **Broker failures:** retried with backoff. A subscriber that still fails is marked FAILED with the error, and `POST /retry` re-runs only those.

## 6. Persistence approach

SQLite through SQLAlchemy 2.0 async (aiosqlite), in WAL mode, with `BEGIN IMMEDIATE` transactions and a busy timeout. This needs zero setup for reviewers. Switching to PostgreSQL only requires changing `DATABASE_URL` (asyncpg).

- Subscribe and exit are each a single transaction: all 12 orders or none.
- Exit sells the user's actual positions, which equal base × multiplier. The subscription row is kept with `active=false`.

In Celery mode, list endpoints (`/api/folios`, `/api/subscriptions`, `/api/admin/users`) are cached in Redis. Invalidation uses a version key, and reads fall back to the DB if Redis is unavailable.

## 7. Testing

```bash
python -m pytest -q
```

Tests use a temporary database and in-process mode, so your `.env` and `folio.db` are untouched. Redis-specific tests are skipped when Redis isn't running.

Covered:

- **Subscription:** 12 BUYs with base × multiplier, persistence, per-user multipliers, duplicate rejected, re-subscribe after exit.
- **Exit:** 12 SELLs, only that subscription affected, double exit rejected.
- **Rebalance:** folio updated, every active subscriber gets SELL + BUY with their own multiplier, inactive users excluded, the admin call returns before the cascade, many subscribers, exit during a pending cascade, broker failure + retry, two swaps in a row.
- **Broker and validation:** receipts, retries, unknown folio, invalid multiplier, invalid / duplicate tickers, invalid base quantity.
- **Celery mode:** cascade through Celery tasks, swap ordering, Redis lock, cache.

## 8. Example API calls

```bash
curl -X POST localhost:8000/api/subscriptions -H "Content-Type: application/json" \
  -d '{"user_id":"user-101","folio_id":"folio-1","multiplier":3}'

curl -X POST localhost:8000/api/subscriptions -H "Content-Type: application/json" \
  -d '{"user_id":"user-202","folio_id":"folio-1","multiplier":5}'

curl -X POST localhost:8000/api/admin/rebalance -H "Content-Type: application/json" \
  -d '{"folio_id":"folio-1","outgoing_ticker":"RELIANCE","incoming_ticker":"IDEA"}'
# 202 {"job_id":"rb-...","subscribers_queued":2,"status":"QUEUED",...}

curl localhost:8000/api/admin/rebalance/rb-...
curl "localhost:8000/api/orders?rebalance_job_id=rb-..."
# user-101: SELL RELIANCE 6, BUY IDEA 6   |   user-202: SELL RELIANCE 10, BUY IDEA 10

curl -X POST localhost:8000/api/subscriptions/sub-.../exit
curl "localhost:8000/api/users/user-202/subscriptions?active=true"
```

On Windows PowerShell use `curl.exe`, or `Invoke-RestMethod`, or Swagger at `/docs`.

## 9. Frontend

`static/index.html` is served at `/`:

- **User panel:** user ID, folio dropdown, multiplier with a quantity preview, Subscribe, active folios with Exit.
- **Admin panel:** folio, existing stock, replace-with, new base quantity, and Trigger rebalance. It shows "Rebalance triggered successfully, OLD → NEW, Active subscribers queued: N" plus live job progress, and a users table.
- **Live order feed:** polls `/api/orders?after_id=` every 1.5 seconds.

## 10. Trade-offs and next steps

- SQLite and `create_all` keep the prototype zero-setup. For production: PostgreSQL + Alembic migrations.
- No authentication. Next step: JWT with an admin role.
- The UI polls for updates. Next step: WebSocket / SSE.
- Rebalance is quantity-based, as specified. A price/weight-based, cash-neutral rebalance would be the next step.
- One Redis serves as broker, lock and cache. In production, run separate instances (broker with `noeviction`, cache with LRU eviction).