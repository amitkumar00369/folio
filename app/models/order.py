from datetime import datetime

from sqlalchemy import CheckConstraint, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base
from app.db.types import UTCDateTime
from app.util.enums import OrderAction, OrderSource, OrderStatus
from app.models.subscription import Subscription


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (CheckConstraint("quantity > 0", name="ck_order_quantity_positive"),)


    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    order_id: Mapped[str] = mapped_column(String(40), unique=True)

    user_id: Mapped[str] = mapped_column(String(64), index=True)
    subscription_id: Mapped[str | None] = mapped_column(
        ForeignKey("subscriptions.id"), index=True
    )
    ticker: Mapped[str] = mapped_column(String(20))
    action: Mapped[OrderAction] = mapped_column(Enum(OrderAction, native_enum=False))
    quantity: Mapped[int] = mapped_column(Integer)
    status: Mapped[OrderStatus] = mapped_column(Enum(OrderStatus, native_enum=False))
    source: Mapped[OrderSource] = mapped_column(Enum(OrderSource, native_enum=False), index=True)
    rebalance_job_id: Mapped[str | None] = mapped_column(
        ForeignKey("rebalance_jobs.id"), index=True
    )
    timestamp: Mapped[datetime] = mapped_column(UTCDateTime)

    subscription: Mapped[Subscription | None] = relationship(lazy="noload")
