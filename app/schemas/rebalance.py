from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models import JobStatus, TaskStatus
from app.util.consants import normalize_ticker


class RebalanceRequest(BaseModel):
    folio_id: str = Field(min_length=1, examples=["folio-1"])
    outgoing_ticker: str = Field(examples=["RELIANCE"])
    incoming_ticker: str = Field(examples=["IDEA"])
    incoming_base_quantity: int | None = Field(
        default=None,
        gt=0,
        description="Base quantity for the new stock. Defaults to the outgoing stock's base quantity.",
    )

    @field_validator("outgoing_ticker", "incoming_ticker")
    @classmethod
    def _ticker(cls, v: str):
        return normalize_ticker(v)


class RebalanceAccepted(BaseModel):
    job_id: str
    folio_id: str
    outgoing_ticker: str
    incoming_ticker: str
    incoming_base_quantity: int
    subscribers_queued: int
    status: JobStatus


class RebalanceTaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    subscription_id: str
    user_id: str
    multiplier: int
    status: TaskStatus
    attempts: int
    error: str | None


class RebalanceJobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    folio_id: str
    outgoing_ticker: str
    outgoing_base_quantity: int
    incoming_ticker: str
    incoming_base_quantity: int
    status: JobStatus
    total_tasks: int
    progress: dict[TaskStatus, int]
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    duration_ms: int | None


class RebalanceJobDetail(RebalanceJobOut):
    tasks: list[RebalanceTaskOut]
