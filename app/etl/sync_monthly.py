from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.extract import extract_facilities
from app.etl.load import load_facilities
from app.etl.transform import transform_facilities

BATCH_SIZE = getattr(settings, "BATCH_SIZE", 50)


def run_etl():
    print("🌏 Starting ETL job...")

    # -------------------------------
    # 1. Extract
    # -------------------------------
    print("🔹 Extracting raw facilities from OpenElectricity API...")
    raw_data = extract_facilities()
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
    updated_total = 0
    skiped_total = 0

    # 1. Manually create the generator object
    db_generator = get_sync_db()
    # 2. Extract the single yielded Session object
    # This executes the generator up to the 'yield db' line.
    db = next(db_generator)
    total = len(transformed)
    for i in range(0, total, BATCH_SIZE):
        batch = transformed[i : i + BATCH_SIZE]

        # Perform upsert
        total_inserted, total_updated, total_skipped = load_facilities(
            batch, db, batch_size=BATCH_SIZE
        )
        inserted_total += total_inserted
        updated_total += total_updated
        skiped_total += total_skipped

    print(
        f"✅ ETL job completed. Total inserted: {inserted_total}, updated: {updated_total}, skipped: {skiped_total} "
    )

    # Task instantiation
    print("🌏 Starting ETL job...")


if __name__ == "__main__":
    run_etl()
