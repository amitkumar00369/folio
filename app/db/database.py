from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool

from app.config import settings


class Base(DeclarativeBase):
    pass


def async_url(url: str):
    if url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def build_engine(url: str):
    url = async_url(url)
    if not url.startswith("sqlite"):
        return create_async_engine(url, pool_pre_ping=True)

    engine = create_async_engine(url, connect_args={"timeout": 30}, poolclass=NullPool)

    @event.listens_for(engine.sync_engine, "connect")
    def on_connect(dbapi_conn, _record):
        dbapi_conn.isolation_level = None
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    @event.listens_for(engine.sync_engine, "begin")
    def on_begin(conn):
        conn.exec_driver_sql("BEGIN IMMEDIATE")

    return engine


engine = build_engine(settings.database_url)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db():
    async with SessionLocal() as db:
        yield db


async def init_db():
    import app.models

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
