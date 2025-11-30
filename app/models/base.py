"""Base SQLAlchemy model with timestamps and audit fields."""

import uuid

from sqlalchemy import TIMESTAMP, Column
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func


# pylint: disable=too-few-public-methods, not-callable
class Base(DeclarativeBase):
    """Declarative base class for SQLAlchemy models."""


class BaseModel(Base):
    """Abstract base model that adds timestamps and user tracking fields."""

    __abstract__ = True  # Don't create table for this class

    # Primary key: unique identifier for the permission
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        index=True,
        comment="Unique identifier for the permission (UUID4)",
    )
    created_at = Column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    created_by = Column(
        TIMESTAMP(timezone=True),
        default=func.now(),
    )

    updated_by = Column(
        TIMESTAMP(timezone=True),
        default=func.now(),
        onupdate=func.now(),
    )
