import asyncio

from app.db.session import get_db
from app.etl.extract import extract_facilities
from app.etl.load import load_facilities
from app.etl.transform import transform_facilities


async def run_etl():
    # ✅ Use 'await' here because extract_facilities is async
    raw_data = await extract_facilities()

    print(f"Total extractcted_facilities : {len(raw_data)}")
    # Transform (still sync)
    transformed = transform_facilities(raw_data)
    print(f"Total transformed_facilities : {len(raw_data)}")

    # # Load (async)
    async for db in get_db():
        await load_facilities(db, transformed)


if __name__ == "__main__":
    asyncio.run(run_etl())
