import asyncio

from etl.extract import extract_facility_data, transform_facility_data
from etl.transform import load_facilities, update_facilities

from app.db.session import async_session


async def run_etl():
    raw_data = await extract_facility_data()
    transformed_data = transform_facility_data(raw_data)

    async with async_session() as db:
        await load_facilities(db, transformed_data)
        await update_facilities(db, transformed_data)


if __name__ == "__main__":
    asyncio.run(run_etl())
