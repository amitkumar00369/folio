from enum import StrEnum


class OrderAction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    EXECUTED = "EXECUTED"
    REJECTED = "REJECTED"


class OrderSource(StrEnum):
    SUBSCRIBE = "SUBSCRIBE"
    EXIT = "EXIT"
    REBALANCE = "REBALANCE"


class JobStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    DONE = "DONE"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
