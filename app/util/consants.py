import re
import uuid
from datetime import UTC, datetime


TICKER_RE = re.compile(r"^[A-Z][A-Z0-9&-]{0,19}$")


def utcnow():
    return datetime.now(UTC)


def new_id(prefix: str):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def normalize_ticker(value: str):
    ticker = value.strip().upper()
    if not ticker:
        raise ValueError("ticker must not be empty")
    if not TICKER_RE.match(ticker):
        raise ValueError(f"'{value}' is not a valid ticker symbol")
    return ticker
