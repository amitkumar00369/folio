import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Folio, FolioStock
from app.schemas.folio import FolioIn

log = logging.getLogger(__name__)


SEED_FOLIOS = [
    {
        "id": "folio-1",
        "name": "Alpha Growth",
        "stocks": [
            ("RELIANCE", 2), ("TCS", 1), ("INFY", 1), ("HDFCBANK", 2),
            ("ICICIBANK", 2), ("SBIN", 3), ("BHARTIARTL", 1), ("LT", 1),
            ("ITC", 2), ("MARUTI", 1), ("AXISBANK", 2), ("SUNPHARMA", 1),
        ],
    },
    {
        "id": "folio-2",
        "name": "Bluechip Core",
        "stocks": [
            ("HDFCBANK", 2), ("RELIANCE", 1), ("TCS", 1), ("INFY", 2),
            ("ICICIBANK", 2), ("KOTAKBANK", 1), ("HINDUNILVR", 1), ("LT", 1),
            ("ITC", 3), ("SBIN", 2), ("BAJFINANCE", 1), ("ASIANPAINT", 1),
        ],
    },
    {
        "id": "folio-3",
        "name": "India Momentum",
        "stocks": [
            ("ADANIENT", 1), ("TATAMOTORS", 3), ("BEL", 5), ("HAL", 1),
            ("TRENT", 1), ("ZOMATO", 6), ("IRFC", 10), ("BAJAJ-AUTO", 1),
            ("M&M", 2), ("POWERGRID", 4), ("NTPC", 4), ("DLF", 2),
        ],
    },
    {
        "id": "folio-4",
        "name": "Dividend Leaders",
        "stocks": [
            ("COALINDIA", 5), ("ITC", 4), ("ONGC", 6), ("POWERGRID", 4),
            ("HINDZINC", 3), ("VEDL", 4), ("NTPC", 4), ("IOC", 8),
            ("GAIL", 6), ("RECLTD", 3), ("PFC", 3), ("BPCL", 4),
        ],
    },
    {
        "id": "folio-5",
        "name": "Tech Innovators",
        "stocks": [
            ("TCS", 1), ("INFY", 2), ("HCLTECH", 2), ("WIPRO", 4),
            ("TECHM", 2), ("LTIM", 1), ("PERSISTENT", 1), ("COFORGE", 1),
            ("MPHASIS", 1), ("KPITTECH", 2), ("TATAELXSI", 1), ("NAUKRI", 1),
        ],
    },
    {
        "id": "folio-6",
        "name": "Consumer Power",
        "stocks": [
            ("HINDUNILVR", 1), ("NESTLEIND", 1), ("BRITANNIA", 1), ("TITAN", 1),
            ("ASIANPAINT", 1), ("DABUR", 4), ("MARICO", 4), ("GODREJCP", 2),
            ("TATACONSUM", 2), ("COLPAL", 1), ("DMART", 1), ("PIDILITIND", 1),
        ],
    },
    {
        "id": "folio-7",
        "name": "Value Select",
        "stocks": [
            ("SBIN", 3), ("BANKBARODA", 6), ("PNB", 10), ("CANBK", 5),
            ("ONGC", 6), ("COALINDIA", 4), ("NMDC", 8), ("SAIL", 10),
            ("GAIL", 6), ("TATASTEEL", 6), ("HINDALCO", 3), ("BPCL", 4),
        ],
    },
]


async def seed_folios(db: AsyncSession):
    created = 0
    for raw in SEED_FOLIOS:
        spec = FolioIn(
            id=raw["id"],
            name=raw["name"],
            stocks=[{"ticker": t, "base_quantity": q} for t, q in raw["stocks"]],
        )
        if await db.get(Folio, spec.id):
            continue
        db.add(
            Folio(
                id=spec.id,
                name=spec.name,
                stocks=[
                    FolioStock(slot=i, ticker=s.ticker, base_quantity=s.base_quantity)
                    for i, s in enumerate(spec.stocks)
                ],
            )
        )
        created += 1
    await db.commit()
    if created:
        log.info("seeded %s folios", created)
    return created
