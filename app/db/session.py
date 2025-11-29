"""Async database session setup for User service."""

from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings

# -------------------------------
# Async Database Engine
# -------------------------------
engine = create_async_engine(
    settings.DATABASE_URL,  # must be async: postgresql+asyncpg://...
    pool_size=10,  # connection pool
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
    echo=False,
    future=True,
)

# -------------------------------
# Async SessionMaker
# -------------------------------
async_session_maker = async_sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


# -------------------------------
# Dependency: Async DB Session
# -------------------------------
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Provide an async database session."""
    async with async_session_maker() as session:
        yield session
