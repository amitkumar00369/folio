from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import FOLIO_SIZE, Folio
from app.schemas.folio import FolioOut
from app.services.cache import FOLIOS, NoCache
from app.services.errors import ConflictError, InvalidRequestError, NotFoundError


class FolioService:
    def __init__(self, db: AsyncSession, cache=None):
        self.db = db
        self.cache = cache or NoCache()

    async def list_folios(self):
        return await self.cache.get_or_load(FOLIOS, ["all"], self._load_all)

    async def get_folio_view(self, folio_id: str):
        return await self.cache.get_or_load(FOLIOS, ["one", folio_id], lambda: self._load_one(folio_id))

    async def _load_all(self):
        folios = await self.db.scalars(select(Folio).order_by(Folio.id))
        return [FolioOut.model_validate(f).model_dump(mode="json") for f in folios]

    async def _load_one(self, folio_id: str):
        folio = await self.get_folio(folio_id)
        return FolioOut.model_validate(folio).model_dump(mode="json")

    async def get_folio(self, folio_id: str):
        folio = await self.db.get(Folio, folio_id)
        if folio is None:
            raise NotFoundError(f"Folio '{folio_id}' not found")
        return folio

    @staticmethod
    def ensure_tradable(folio: Folio):
        if len(folio.stocks) != FOLIO_SIZE:
            raise InvalidRequestError(
                f"Folio '{folio.id}' has {len(folio.stocks)} stocks, expected {FOLIO_SIZE}"
            )
        for stock in folio.stocks:
            if not stock.ticker or stock.base_quantity <= 0:
                raise InvalidRequestError(f"Folio '{folio.id}' has an invalid stock entry")

    async def lock_folio(self, folio: Folio, bump: bool = False):
        new_version = folio.version + 1 if bump else folio.version
        
        result = await self.db.execute(
            update(Folio)
            .where(Folio.id == folio.id, Folio.version == folio.version)
            .values(version=new_version)
        )
        if result.rowcount != 1:
            await self.db.rollback()
            raise ConflictError(f"Folio '{folio.id}' was modified concurrently, please retry")
