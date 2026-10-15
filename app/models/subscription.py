from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base
from app.db.types import UTCDateTime
from app.models.folio import Folio
from app.util.consants import utcnow


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = (CheckConstraint("multiplier > 0", name="ck_multiplier_positive"),)

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    folio_id: Mapped[str] = mapped_column(ForeignKey("folios.id"), index=True)
    multiplier: Mapped[int] = mapped_column(Integer)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    exited_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    folio: Mapped[Folio] = relationship(lazy="selectin")
    positions: Mapped[list["Position"]] = relationship(
        back_populates="subscription",
        cascade="all, delete-orphan",
        order_by="Position.id",
        lazy="selectin",
    )

    @property
    def folio_name(self):
        return self.folio.name

    def position_for(self, ticker: str):
        return next((p for p in self.positions if p.ticker == ticker), None)

Index(
    "uq_active_user_folio",
    Subscription.user_id,
    Subscription.folio_id,
    unique=True,
    sqlite_where=Subscription.active.is_(True),
    postgresql_where=Subscription.active.is_(True),
)


class Position(Base):

    __tablename__ = "positions"
    __table_args__ = (
        UniqueConstraint("subscription_id", "ticker", name="uq_position_ticker"),
        CheckConstraint("quantity > 0", name="ck_position_quantity_positive"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscription_id: Mapped[str] = mapped_column(
        ForeignKey("subscriptions.id", ondelete="CASCADE"), index=True
    )
    ticker: Mapped[str] = mapped_column(String(20))
    quantity: Mapped[int] = mapped_column(Integer)

    subscription: Mapped[Subscription] = relationship(back_populates="positions")
