"""Database session setup for User service with sync & async support."""

from typing import AsyncGenerator, Generator

# SQLAlchemy Imports
# We now import 'sessionmaker' from sqlalchemy.orm for BOTH sync and async sessions
from sqlalchemy import create_engine

# Specific imports for Async functionality
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings

# ------------------------------------
# 🚀 Async Database Engine & Session
# ------------------------------------
engine = create_async_engine(
    settings.DATABASE_URL,  # must be async: postgresql+asyncpg://...
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
    echo=False,
    future=True,  # Recommended for 2.0 style
)

# **FIXED**: Using top-level 'sessionmaker' imported from 'sqlalchemy.orm'
# The 'class_=AsyncSession' argument makes it an async session factory.
async_session_maker = sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,  # This tells the maker to produce AsyncSession objects
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Provide an async database session dependency."""
    # This uses the Python async context manager protocol
    async with async_session_maker() as session:
        yield session


# -----------------------------------
# ⚙️ Sync Database Engine & Session
# -----------------------------------
sync_engine = create_engine(
    settings.SYNC_DATABASE_URL,  # e.g., postgresql+psycopg2://...
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
    echo=False,
    future=True,  # Recommended for 2.0 style
)

# Using the same 'sessionmaker' for the sync engine
SyncSessionLocal = sessionmaker(
    bind=sync_engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    class_=Session,  # This tells the maker to produce standard Session objects
)


def get_sync_db() -> Generator[Session, None, None]:
    """Provide a sync database session dependency."""
    db: Session = SyncSessionLocal()
    try:
        yield db
    finally:
        # Standard synchronous session close
        db.close()
