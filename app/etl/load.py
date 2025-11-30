from typing import Any, Dict, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.facility import Facility


async def load_facilities(db: AsyncSession, facilities: List[Dict[str, Any]]):
    """
    Upsert facilities into DB.
    If a facility exists (name + network_id), update it; else insert.
    """
    for fac in facilities:
        stmt = select(Facility).where(
            Facility.name == fac["name"], Facility.network_id == fac["network_id"]
        )
        result = await db.execute(stmt)
        obj = result.scalar_one_or_none()

        if obj:
            # Update existing
            for key, value in fac.items():
                setattr(obj, key, value)
        else:
            # Insert new
            db.add(Facility(**fac))

    await db.commit()
