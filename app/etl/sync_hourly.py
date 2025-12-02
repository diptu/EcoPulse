import asyncio

from db.session import get_sync_db
from etl.extract import extract_facility_data, transform_facility_data
from etl.load import load_facilities


async def run_etl():
    raw_data = extract_facility_data()
    transformed_data = transform_facility_data(raw_data)

    db_generator = get_sync_db()
    # 2. Extract the single yielded Session object
    # This executes the generator up to the 'yield db' line.
    db = next(db_generator)
    load_facilities(db, transformed_data)
    # await update_facilities(db, transformed_data)


if __name__ == "__main__":
    asyncio.run(run_etl())
