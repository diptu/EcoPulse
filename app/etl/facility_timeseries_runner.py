# app/etl/facility_timeseries_runner.py
from datetime import datetime, timedelta, timezone

from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.facility_timeseries import FacilityTimeseriesETL

BATCH_SIZE = getattr(settings, "BATCH_SIZE", 100)
FACILITY_SIZE = getattr(settings, "FACILITY_SIZE", 25)


def run_etl(
    start_time: datetime = None, end_time: datetime = None, facility_size=FACILITY_SIZE
):
    print("🌏 Starting Facility Timeseries ETL job...")

    # Get DB session first
    db_generator = get_sync_db()
    db = next(db_generator)

    # Pass db to ETL
    etl = FacilityTimeseriesETL(
        db=db,
        start_time=start_time,
        end_time=end_time,
        interval="1h",
        num_facility=facility_size,  # 30 worked, use 25 to be in safe zone
    )

    print("🔹 Extracting facility timeseries data from OpenElectricity API...")
    raw_data = etl.extract()
    print(f"✅ Extracted rows: {len(raw_data)}")

    transformed = etl.transform(raw_data)
    print(f"✅ Transformed rows: {len(transformed)}")

    inserted, updated, skipped = etl.load(transformed, db, batch_size=BATCH_SIZE)
    print(
        f"✅ Facility Timeseries ETL completed. Total inserted: {inserted}, "
        f"updated: {updated}, skipped: {skipped}"
    )


if __name__ == "__main__":
    LAG_HOURS = 0
    HOURS_BACK = 1 + LAG_HOURS
    end_time = (
        datetime.now(timezone.utc)
        .replace(minute=0, second=0, microsecond=0)
        .replace(tzinfo=None)
    )
    start_time = (end_time - timedelta(hours=HOURS_BACK)).replace(tzinfo=None)
    print(f"Fetching data from {start_time.isoformat()} to {end_time.isoformat()} UTC")
    run_etl(start_time=start_time, end_time=end_time, facility_size=FACILITY_SIZE)
