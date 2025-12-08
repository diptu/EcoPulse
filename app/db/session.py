# app/core/db.py

"""Database session setup for async & sync access (Python 3.12+ compatible)
with lazy engine initialization."""

from typing import AsyncGenerator, Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

# Assuming app.core.config.settings is available and loaded
from app.core.config import settings

# ---------------------------
# 🚀 Async Database Engine (lazy)
# ---------------------------

# Type hint using the Engine type from SQLAlchemy is more accurate
_async_engine: Optional[create_async_engine] = None
_async_session_maker: Optional[sessionmaker] = None


def get_async_engine() -> sessionmaker:
    """
    Initializes and returns the AsyncSession factory (sessionmaker)
    lazily upon first call.
    """
    global _async_engine, _async_session_maker

    if _async_engine is None:
        if not settings.DATABASE_URL:
            # We use RuntimeError here because this indicates a configuration
            # failure at runtime after settings were expected to be loaded.
            raise RuntimeError("DATABASE_URL must be set before creating async engine")

        _async_engine = create_async_engine(
            settings.DATABASE_URL,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
            echo=False,
            future=True,
        )

        _async_session_maker = sessionmaker(
            bind=_async_engine,
            expire_on_commit=False,
            class_=AsyncSession,  # Explicitly specify the factory class
        )

    # Note: We return the session maker, not the engine itself.
    return _async_session_maker


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Async database session dependency for FastAPI/frameworks.
    Retrieves the session factory lazily.
    """
    session_maker = get_async_engine()

    async with session_maker() as session:
        yield session


# ---------------------------
# ⚙️ Sync Database Engine (lazy)
# ---------------------------

# Type hint using the Engine type from SQLAlchemy is more accurate
_sync_engine: Optional[create_engine] = None
_sync_session_maker: Optional[sessionmaker] = None


def get_sync_engine() -> sessionmaker:
    """
    Initializes and returns the standard Session factory (sessionmaker)
    lazily upon first call.
    """
    global _sync_engine, _sync_session_maker

    if _sync_engine is None:
        if not settings.SYNC_DATABASE_URL:
            # Use RuntimeError for configuration issues here too
            raise RuntimeError(
                "SYNC_DATABASE_URL must be set before creating sync engine"
            )

        _sync_engine = create_engine(
            settings.SYNC_DATABASE_URL,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
            echo=False,
            future=True,
        )

        _sync_session_maker = sessionmaker(
            bind=_sync_engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
            class_=Session,  # Explicitly specify the factory class
        )

    # Note: We return the session maker, not the engine itself.
    return _sync_session_maker


def get_sync_db() -> Generator[Session, None, None]:
    """
    Sync database session dependency for Airflow tasks/legacy components.
    Retrieves the session factory lazily and manages the session lifecycle.
    """
    session_maker = get_sync_engine()

    db: Session = session_maker()

    try:
        yield db
    finally:
        # Crucial to close the sync session manually
        db.close()
