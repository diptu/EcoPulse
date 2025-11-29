from sqlalchemy.ext.asyncio import AsyncSession

from app.models.facility import Facility


# ------------------- Load -------------------
async def load_facilities(db: AsyncSession, data: list):
    """Insert new records."""
    for item in data:
        exists = await db.get(Facility, item["id"])
        if not exists:
            db.add(Facility(**item))
    await db.commit()


# ------------------- Update -------------------
async def update_facilities(db: AsyncSession, data: list):
    """Update existing records."""
    for item in data:
        obj = await db.get(Facility, item["id"])
        if obj:
            for key, value in item.items():
                setattr(obj, key, value)
    await db.commit()
