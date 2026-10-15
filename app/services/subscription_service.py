import logging

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OrderAction, OrderSource, Subscription
from app.schemas.subscription import SubscriptionOut
from app.services.broker_service import MockBroker
from app.services.cache import SUBSCRIPTIONS, NoCache
from app.services.errors import ConflictError, InvalidRequestError, NotFoundError
from app.services.folio_service import FolioService
from app.services.locks import subscription_locks
from app.services.order_service import OrderService
from app.util.consants import new_id, utcnow

log = logging.getLogger(__name__)


class SubscriptionService:
    def __init__(self, db: AsyncSession, broker: MockBroker | None = None, cache=None, locks=None):
        self.db = db
        self.cache = cache or NoCache()
        self.locks = locks or subscription_locks
        self.folios = FolioService(db)
        self.orders = OrderService(db, broker)

    async def subscribe(self, user_id: str, folio_id: str, multiplier: int):
        if multiplier <= 0:
            raise InvalidRequestError("multiplier must be a positive integer")

        folio = await self.folios.get_folio(folio_id)
        self.folios.ensure_tradable(folio)
        await self.folios.lock_folio(folio)

        already = await self.db.scalar(
            select(Subscription.id).where(
                Subscription.user_id == user_id,
                Subscription.folio_id == folio_id,
                Subscription.active.is_(True),
            )
        )
        if already:
            await self.db.rollback()
            raise ConflictError(f"{user_id} already has an active subscription ({already}) to {folio_id}")

        sub = Subscription(
            id=new_id("sub"), user_id=user_id, folio_id=folio.id, multiplier=multiplier, active=True
        )
        sub.folio = folio
        sub.positions = []
        self.db.add(sub)

        try:
            orders = []
            for stock in folio.stocks:
                orders.append(
                    await self.orders.execute_trade(
                        sub, stock.ticker, OrderAction.BUY,
                        stock.base_quantity * multiplier, OrderSource.SUBSCRIBE,
                    )
                )
            await self.db.commit()
        except IntegrityError as exc:
            await self.db.rollback()
            if "uq_active_user_folio" in str(exc.orig) or "subscriptions.user_id" in str(exc.orig):
                raise ConflictError(f"{user_id} already has an active subscription to {folio_id}") from exc
            raise
        except Exception:
            await self.db.rollback()
            raise

        await self.cache.invalidate(SUBSCRIPTIONS)
        log.info("subscribed %s to %s at %sx (%s orders)", user_id, folio_id, multiplier, len(orders))
        return sub, orders

    async def exit(self, subscription_id: str):
        async with self.locks.hold(subscription_id):
            sub = await self.get(subscription_id)
            if not sub.active:
                raise ConflictError(f"Subscription '{subscription_id}' is already exited")
            await self.folios.get_folio(sub.folio_id)

            sub.active = False
            sub.exited_at = utcnow()

            try:
                orders = []
                for pos in list(sub.positions):
                    orders.append(
                        await self.orders.execute_trade(
                            sub, pos.ticker, OrderAction.SELL, pos.quantity, OrderSource.EXIT
                        )
                    )
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise

        await self.cache.invalidate(SUBSCRIPTIONS)
        log.info("exited %s (%s, %s orders)", subscription_id, sub.user_id, len(orders))
        return sub, orders

    async def get(self, subscription_id: str):
        sub = await self.db.get(Subscription, subscription_id)
        if sub is None:
            raise NotFoundError(f"Subscription '{subscription_id}' not found")
        return sub

    async def list_for_user(self, user_id: str, active: bool | None = None):
        stmt = (
            select(Subscription)
            .where(Subscription.user_id == user_id)
            .order_by(Subscription.created_at.desc())
        )
        if active is not None:
            stmt = stmt.where(Subscription.active.is_(active))
        return list(await self.db.scalars(stmt))

    async def list_all(
        self,
        folio_id: str | None = None,
        active: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ):
        async def load():
            stmt = select(Subscription).order_by(Subscription.created_at.desc())
            if folio_id:
                stmt = stmt.where(Subscription.folio_id == folio_id)
            if active is not None:
                stmt = stmt.where(Subscription.active.is_(active))
            subs = await self.db.scalars(stmt.limit(limit).offset(offset))
            return [SubscriptionOut.model_validate(s).model_dump(mode="json") for s in subs]

        return await self.cache.get_or_load(SUBSCRIPTIONS, ["list", folio_id, active, limit, offset], load)

    async def list_users(self, active_only: bool = False):
        async def load():
            rows = (
                await self.db.execute(
                    select(Subscription.user_id, Subscription.folio_id, Subscription.active, Subscription.created_at)
                    .order_by(Subscription.created_at)
                )
            ).all()

            users: dict[str, dict] = {}
            for user_id, folio_id, active, created_at in rows:
                u = users.setdefault(user_id, {
                    "user_id": user_id,
                    "total_subscriptions": 0,
                    "active_subscriptions": 0,
                    "active_folios": [],
                    "first_subscribed_at": created_at.isoformat(),
                })
                u["total_subscriptions"] += 1
                if active:
                    u["active_subscriptions"] += 1
                    u["active_folios"].append(folio_id)
            return list(users.values())

        users = await self.cache.get_or_load(SUBSCRIPTIONS, ["users"], load)
        if active_only:
            users = [u for u in users if u["active_subscriptions"] > 0]
        return users
