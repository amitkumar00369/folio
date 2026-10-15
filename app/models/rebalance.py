from datetime import datetime

from sqlalchemy import Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base
from app.db.types import UTCDateTime
from app.util.enums import JobStatus, TaskStatus
from app.models.subscription import Subscription
from app.util.consants import utcnow


class RebalanceJob(Base):

    __tablename__ = "rebalance_jobs"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    folio_id: Mapped[str] = mapped_column(ForeignKey("folios.id"), index=True)
    outgoing_ticker: Mapped[str] = mapped_column(String(20))
    outgoing_base_quantity: Mapped[int] = mapped_column(Integer)
    incoming_ticker: Mapped[str] = mapped_column(String(20))
    incoming_base_quantity: Mapped[int] = mapped_column(Integer)

    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, native_enum=False), index=True)
    total_tasks: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    tasks: Mapped[list["RebalanceTask"]] = relationship(
        back_populates="job", cascade="all, delete-orphan", order_by="RebalanceTask.id"
    )


class RebalanceTask(Base):

    __tablename__ = "rebalance_tasks"
    __table_args__ = (UniqueConstraint("job_id", "subscription_id", name="uq_job_subscription"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("rebalance_jobs.id"), index=True)
    subscription_id: Mapped[str] = mapped_column(ForeignKey("subscriptions.id"))
    user_id: Mapped[str] = mapped_column(String(64))
    multiplier: Mapped[int] = mapped_column(Integer)

    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, native_enum=False), default=TaskStatus.PENDING, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime, default=utcnow, onupdate=utcnow
    )

    job: Mapped[RebalanceJob] = relationship(back_populates="tasks", lazy="selectin")
    subscription: Mapped[Subscription] = relationship(lazy="selectin")
