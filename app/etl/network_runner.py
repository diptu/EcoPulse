# app/etl/runner.py
from datetime import datetime, timedelta

from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.network import NetworkETL

BATCH_SIZE = getattr(settings, "BATCH_SIZE", 50)


def run_etl(start_time: datetime = None, end_time: datetime = None):
    print("🌏 Starting ETL job...")
    etl = NetworkETL(start_time=start_time, end_time=end_time, interval="1h")

    # -------------------------------
    # 1. Extract
    # -------------------------------
    print("🔹 Extracting raw network data from OpenElectricity API...")
    # 1️⃣ Extract
    raw_data = etl.extract()

    print(f"✅ Extracted netwrok rows;{len(raw_data)}")

    # 2️⃣ Transform
    transformed = etl.transform(raw_data)

    print(f"✅ Transfformed netwrok rows;{len(transformed)}")

    # 3️⃣ Load
    inserted_total = updated_total = skipped_total = 0

    db_generator = get_sync_db()
    db = next(db_generator)  # Get the single Session object

    inserted, updated, skipped = etl.load(transformed, db, batch_size=BATCH_SIZE)

    inserted_total += inserted
    updated_total += updated
    skipped_total += skipped

    print(
        f"✅ Network ETL completed. Total inserted: {inserted_total}, "
        f"updated: {updated_total}, skipped: {skipped_total}"
    )


if __name__ == "__main__":
    # Optional: run for last 24 hours by default
    LAG_HOURS = 1
    HOURS_BACK = 1
    TOTAL_HOURS_BACK = LAG_HOURS + HOURS_BACK
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=TOTAL_HOURS_BACK)
    run_etl(start_time=start_time, end_time=end_time)
