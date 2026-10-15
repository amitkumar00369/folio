from sqlalchemy import select, update
from sqlalchemy.orm import Session, selectinload

from app.models import FOLIO_SIZE, Folio
from app.services.errors import ConflictError, InvalidRequestError, NotFoundError


def list_folios(db: Session):
    return list(db.scalars(select(Folio).options(selectinload(Folio.stocks)).order_by(Folio.id)))


def get_folio(db: Session, folio_id: str):
    folio = db.get(Folio, folio_id, options=[selectinload(Folio.stocks)])
    if folio is None:
        raise NotFoundError(f"Folio '{folio_id}' not found")
    return folio


def ensure_tradable(folio: Folio):
    if len(folio.stocks) != FOLIO_SIZE:
        raise InvalidRequestError(
            f"Folio '{folio.id}' has {len(folio.stocks)} stocks, expected {FOLIO_SIZE}"
        )
    for stock in folio.stocks:
        if not stock.ticker or stock.base_quantity <= 0:
            raise InvalidRequestError(f"Folio '{folio.id}' has an invalid stock entry")


def lock_folio(db: Session, folio: Folio, *, bump: bool = False):

    new_version = folio.version + 1 if bump else folio.version
    result = db.execute(
        update(Folio)
        .where(Folio.id == folio.id, Folio.version == folio.version)
        .values(version=new_version)
    )
    if result.rowcount != 1:
        db.rollback()
        raise ConflictError(f"Folio '{folio.id}' was modified concurrently, please retry")
