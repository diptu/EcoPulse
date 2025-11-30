# app/crud/base.py

"""Async-only generic BaseCRUD for SQLAlchemy models with batch operations."""

from typing import Any, Dict, Generic, List, Optional, Type, TypeVar
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.functions import count

T = TypeVar("T")  # Model type
C = TypeVar("C")  # Create schema type
U = TypeVar("U")  # Update schema type


class AlreadyExistsError(Exception):
    """Raised when a create operation violates unique constraints."""


class BaseCRUD(Generic[T, C, U]):
    """Async-only generic base CRUD class for SQLAlchemy models."""

    def __init__(self, model: Type[T], pk_field: str = "id"):
        self.model = model
        self.pk_field = pk_field  # primary key column name

    async def _commit_refresh(self, db: AsyncSession, db_obj: T) -> T:
        """Commit and refresh a SQLAlchemy object with rollback on failure."""
        try:
            db.add(db_obj)
            await db.commit()
            await db.refresh(db_obj)
            return db_obj
        except IntegrityError as e:
            await db.rollback()
            raise AlreadyExistsError(
                f"{self.model.__name__} with given data already exists."
            ) from e

    async def _update_commit_refresh(
        self, db: AsyncSession, db_obj: T, update_data: Dict[str, Any]
    ) -> T:
        """Update fields of a SQLAlchemy object and commit with refresh."""
        for field, value in update_data.items():
            setattr(db_obj, field, value)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def get(self, db: AsyncSession, obj_id: UUID) -> Optional[T]:
        """Retrieve a single object by its ID."""
        return await db.get(self.model, obj_id)

    async def get_all(
        self, db: AsyncSession, skip: int = 0, limit: int = 100
    ) -> List[T]:
        """Retrieve multiple objects with pagination."""
        result = await db.execute(select(self.model).offset(skip).limit(limit))
        return result.scalars().all()

    async def create(self, db: AsyncSession, obj_in: C) -> T:
        """Create a new record."""
        db_obj = self.model(**obj_in.dict())  # type: ignore
        return await self._commit_refresh(db, db_obj)

    async def bulk_create(self, db: AsyncSession, obj_list: List[C]) -> List[T]:
        """Create multiple records in a single transaction."""
        db_objs = [self.model(**obj.dict()) for obj in obj_list]  # type: ignore
        try:
            db.add_all(db_objs)
            await db.commit()
            for obj in db_objs:
                await db.refresh(obj)
            return db_objs
        except IntegrityError as e:
            await db.rollback()
            raise AlreadyExistsError(
                f"One or more {self.model.__name__} objects already exist."
            ) from e

    async def update(self, db: AsyncSession, db_obj: T, obj_in: U) -> T:
        """Update an existing record."""
        update_data: Dict[str, Any] = obj_in.dict(exclude_unset=True)  # type: ignore
        return await self._update_commit_refresh(db, db_obj, update_data)

    async def delete(self, db: AsyncSession, obj_id: UUID) -> Optional[T]:
        """Delete a record by ID."""
        db_obj = await self.get(db, obj_id)
        if db_obj:
            await db.delete(db_obj)
            await db.commit()
        return db_obj

    async def count(self, db: AsyncSession) -> int:
        """Return total number of records."""
        stmt = select(count()).select_from(self.model)
        result = await db.execute(stmt)
        return result.scalar_one()
