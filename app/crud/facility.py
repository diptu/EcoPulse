"""
Async CRUD operations for Facility model using BaseCRUD.
"""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import BaseCRUD
from app.models.facility import Facility
from app.schemas.facility import FacilityCreate, FacilityUpdate


class FacilityCRUD(BaseCRUD[Facility, FacilityCreate, FacilityUpdate]):
    """Async CRUD for Facility model."""

    # -----------------------------------------------------
    # Get Facility by Code
    # -----------------------------------------------------
    async def get_by_code(self, db: AsyncSession, code: str) -> Optional[Facility]:
        """Retrieve a facility by facility code (unique identifier)."""
        stmt = select(Facility).where(Facility.code == code)
        result = await db.execute(stmt)
        return result.scalars().first()

    # -----------------------------------------------------
    # Create Facility
    # -----------------------------------------------------
    async def create(self, db: AsyncSession, obj_in: FacilityCreate) -> Facility:
        """Create a new facility."""
        db_obj = Facility(**obj_in.model_dump())
        return await self._commit_refresh(db, db_obj)

    # -----------------------------------------------------
    # Update Facility
    # -----------------------------------------------------
    async def update(
        self,
        db: AsyncSession,
        db_obj: Facility,
        obj_in: FacilityUpdate,
    ) -> Facility:
        """Update a facility's fields."""
        update_data = obj_in.model_dump(exclude_unset=True)
        return await self._update_commit_refresh(db, db_obj, update_data)

    # -----------------------------------------------------
    # Upsert (create or update)
    # -----------------------------------------------------
    async def upsert(
        self,
        db: AsyncSession,
        code: str,
        obj_in: FacilityCreate | FacilityUpdate,
    ) -> Facility:
        """
        Create a facility if it does not exist, otherwise update it.
        Useful for ETL ingestion to keep API idempotent.
        """
        existing = await self.get_by_code(db, code)

        if existing:
            return await self.update(db, existing, obj_in)

        return await self.create(db, obj_in)


facility_crud = FacilityCRUD(Facility)
