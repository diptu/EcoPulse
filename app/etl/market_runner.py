from datetime import datetime

from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.market import MarketETL

BATCH_SIZE = getattr(settings, "BATCH_SIZE", 50)


def run_etl(start_time: datetime = None, end_time: datetime = None):
    print("🌏 Starting Market ETL job...")

    etl = MarketETL(start_time=start_time, end_time=end_time, interval="1h")

    # -------------------------------
    # 1. Extract
    # -------------------------------
    print("🔹 Extracting market/curtailment data from OpenElectricity API...")
    raw_data = etl.extract()
    print(f"✅ Extracted {len(raw_data)} market rows.")

    # -------------------------------
    # 2. Transform
    # -------------------------------
    transformed = etl.transform(raw_data)
    print(f"✅ Transformed {len(transformed)} market rows.")

    # -------------------------------
    # 3. Load
    # -------------------------------
    db_generator = get_sync_db()
    db = next(db_generator)

    inserted, updated, skipped = etl.load(transformed, db, batch_size=BATCH_SIZE)
    print(
        f"✅ Market ETL completed. Total inserted: {inserted}, "
        f"updated: {updated}, skipped: {skipped}"
    )


if __name__ == "__main__":
    run_etl()
