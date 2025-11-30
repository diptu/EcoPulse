import asyncio

from app.core.config import settings
from app.db.session import get_db
from app.etl.extract import extract_facilities
from app.etl.load import load_facilities
from app.etl.transform import transform_facilities

BATCH_SIZE = getattr(settings, "ETL_BATCH_SIZE", 100)


async def run_etl():
    print("🌏 Starting ETL job...")

    # -------------------------------
    # 1. Extract
    # -------------------------------
    print("🔹 Extracting raw facilities from OpenElectricity API...")
    raw_data = await extract_facilities()
    print(f"✅ Extracted {len(raw_data)} facilities")

    # -------------------------------
    # 2. Transform
    # -------------------------------
    print("🔹 Transforming facilities...")
    transformed = transform_facilities(raw_data)
    print(f"✅ Transformed {len(transformed)} facilities")

    # -------------------------------
    # 3. Load (Batch Upsert)
    # -------------------------------
    inserted_total = 0
    async for db in get_db():
        total = len(transformed)
        for i in range(0, total, BATCH_SIZE):
            batch = transformed[i : i + BATCH_SIZE]

            # Perform upsert
            await load_facilities(batch, db, batch_size=BATCH_SIZE)
            inserted_total += len(batch)

    print(f"✅ ETL job completed. Total processed: {inserted_total}")


if __name__ == "__main__":
    asyncio.run(run_etl())
