from sqlalchemy import CheckConstraint, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base

FOLIO_SIZE = 12


class Folio(Base):
    __tablename__ = "folios"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    stocks: Mapped[list["FolioStock"]] = relationship(
        back_populates="folio",
        cascade="all, delete-orphan",
        order_by="FolioStock.slot",
        lazy="selectin",
    )

    def find_stock(self, ticker: str):
        return next((s for s in self.stocks if s.ticker == ticker), None)

    @property
    def tickers(self):
        return {s.ticker for s in self.stocks}


class FolioStock(Base):
    __tablename__ = "folio_stocks"
    __table_args__ = (
        UniqueConstraint("folio_id", "ticker", name="uq_folio_ticker"),
        UniqueConstraint("folio_id", "slot", name="uq_folio_slot"),
        CheckConstraint("base_quantity > 0", name="ck_base_quantity_positive"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    folio_id: Mapped[str] = mapped_column(ForeignKey("folios.id", ondelete="CASCADE"), index=True)
    slot: Mapped[int] = mapped_column(Integer)
    ticker: Mapped[str] = mapped_column(String(20))
    base_quantity: Mapped[int] = mapped_column(Integer)

    folio: Mapped[Folio] = relationship(back_populates="stocks")
