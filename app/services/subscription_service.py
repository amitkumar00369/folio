import logging

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.models import Order, OrderAction, OrderSource, Subscription
from app.services import folio_service
from app.services.broker_service import MockBroker
from app.services.errors import ConflictError, InvalidRequestError, NotFoundError
from app.services.locks import subscription_locks
from app.services.order_service import execute_trade
from app.util.consants import new_id, utcnow

log = logging.getLogger(__name__)


def subscribe(
    db: Session, broker: MockBroker, user_id: str, folio_id: str, multiplier: int
):
    if multiplier <= 0:
        raise InvalidRequestError("multiplier must be a positive integer")

    folio = folio_service.get_folio(db, folio_id)
    folio_service.ensure_tradable(folio)
    folio_service.lock_folio(db, folio)

    already = db.scalar(
        select(Subscription.id).where(
            Subscription.user_id == user_id,
            Subscription.folio_id == folio_id,
            Subscription.active.is_(True),
        )
    )
    if already:
        db.rollback()
        raise ConflictError(f"{user_id} already has an active subscription ({already}) to {folio_id}")

    sub = Subscription(
        id=new_id("sub"), user_id=user_id, folio_id=folio.id, multiplier=multiplier, active=True
    )
    db.add(sub)

    try:
        orders = [
            execute_trade(
                db, broker, sub, stock.ticker, OrderAction.BUY,
                stock.base_quantity * multiplier, OrderSource.SUBSCRIBE,
            )
            for stock in folio.stocks
        ]
        db.commit()
    except IntegrityError as exc:
        db.rollback()

        if "uq_active_user_folio" in str(exc.orig) or "subscriptions.user_id" in str(exc.orig):
            raise ConflictError(f"{user_id} already has an active subscription to {folio_id}") from exc
        raise
    except Exception:
        db.rollback()
        raise

    log.info("subscribed %s to %s at %sx (%s orders)", user_id, folio_id, multiplier, len(orders))
    return sub, orders


def exit_subscription(db: Session, broker: MockBroker, subscription_id: str):
    with subscription_locks.hold(subscription_id):
        sub = get_subscription(db, subscription_id)
        if not sub.active:
            raise ConflictError(f"Subscription '{subscription_id}' is already exited")
        folio_service.get_folio(db, sub.folio_id)

        sub.active = False
        sub.exited_at = utcnow()

        try:
            orders = [
                execute_trade(db, broker, sub, pos.ticker, OrderAction.SELL, pos.quantity, OrderSource.EXIT)
                for pos in list(sub.positions)
            ]
            db.commit()
        except Exception:
            db.rollback()
            raise

    log.info("exited %s (%s, %s orders)", subscription_id, sub.user_id, len(orders))
    return sub, orders
def list_subscriptions(
    db: Session,
    folio_id: str | None = None,
    active: bool | None = None,
    limit: int = 100,
    offset: int = 0,
):
    stmt = (
        select(Subscription)
        .options(selectinload(Subscription.positions), selectinload(Subscription.folio))
        .order_by(Subscription.created_at.desc())
    )
    if folio_id:
        stmt = stmt.where(Subscription.folio_id == folio_id)
    if active is not None:
        stmt = stmt.where(Subscription.active.is_(active))
    return list(db.scalars(stmt.limit(limit).offset(offset)))


def get_subscription(db: Session, subscription_id: str):
    sub = db.get(
        Subscription,
        subscription_id,
        options=[selectinload(Subscription.positions), selectinload(Subscription.folio)],
    )
    if sub is None:
        raise NotFoundError(f"Subscription '{subscription_id}' not found")
    return sub


def list_user_subscriptions(db: Session, user_id: str, active: bool | None = None):
    stmt = (
        select(Subscription)
        .where(Subscription.user_id == user_id)
        .options(selectinload(Subscription.positions), selectinload(Subscription.folio))
        .order_by(Subscription.created_at.desc())
    )
    if active is not None:
        stmt = stmt.where(Subscription.active.is_(active))
    return list(db.scalars(stmt))
