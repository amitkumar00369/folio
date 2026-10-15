from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.models import FOLIO_SIZE
from app.util.consants import normalize_ticker


class StockIn(BaseModel):
    ticker: str
    base_quantity: int = Field(gt=0)

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, v: str) -> str:
        return normalize_ticker(v)


class FolioIn(BaseModel):


    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    stocks: list[StockIn] = Field(min_length=FOLIO_SIZE, max_length=FOLIO_SIZE)

    @model_validator(mode="after")
    def tickers_unique(self):
        tickers = [s.ticker for s in self.stocks]
        dupes = {t for t in tickers if tickers.count(t) > 1}
        if dupes:
            raise ValueError(f"duplicate tickers in folio {self.id}: {sorted(dupes)}")
        return self


class StockOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker: str
    base_quantity: int


class FolioSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    version: int


class FolioOut(FolioSummary):
    stocks: list[StockOut]
