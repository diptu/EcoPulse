from datetime import datetime, timedelta

from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.facility import FacilityETL

BATCH_SIZE = getattr(settings, "BATCH_SIZE", 50)


def run_etl(start_time: datetime = None, end_time: datetime = None):
    print("🌏 Starting ETL job...")
    etl = FacilityETL(start_time=start_time, end_time=end_time, interval="1h")

    # -------------------------------
    # 1. Extract
    # -------------------------------
    print("🔹 Extracting raw facility data from OpenElectricity API...")
    raw_data = etl.extract()
    print(f"✅ Extracted facility rows: {len(raw_data)}")

    # -------------------------------
    # 2. Transform
    # -------------------------------
    transformed = etl.transform(raw_data)
    print(f"✅ Transformed facility rows: {len(transformed)}")

    # -------------------------------
    # 3. Load
    # -------------------------------
    inserted_total = updated_total = skipped_total = 0
    db_generator = get_sync_db()
    db = next(db_generator)  # Get the single Session object

    inserted, updated, skipped = etl.load(transformed, db, batch_size=BATCH_SIZE)
    inserted_total += inserted
    updated_total += updated
    skipped_total += skipped

    print(
        f"✅ Facility ETL completed. Total inserted: {inserted_total}, "
        f"updated: {updated_total}, skipped: {skipped_total}"
    )


if __name__ == "__main__":
    # Optional: run for last 2 hours by default
    HOURS_BACK = 2
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=HOURS_BACK)
    run_etl(start_time=start_time, end_time=end_time)
