"""Base SQLAlchemy model with timestamps and audit fields."""

import uuid

from sqlalchemy import TIMESTAMP, Column
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

# SQLAlchemy 1.4-compatible base class
Base = declarative_base()


class BaseModel(Base):
    """Abstract base model that adds timestamps and user tracking fields."""

    __abstract__ = True  # Don't create table for this class

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        index=True,
        comment="Unique identifier for the record (UUID4)",
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
