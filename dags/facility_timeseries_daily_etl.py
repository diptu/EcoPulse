# dags/facility_timeseries_daily_etl.py
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.facility_timeseries import FacilityTimeseriesETL

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# -------------------------------
# DAG default args
# -------------------------------
default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BATCH_SIZE = settings.BATCH_SIZE
LAG_HOURS = 0
HOURS_BACK = 24


# -------------------------------
# DAG Definition
# -------------------------------
@dag(
    dag_id="facility_timeseries_daily_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["opennem", "facility_timeseries"],
)
def facility_timeseries_daily_pipeline():
    @task()
    def run_etl():
        """
        Full ETL task: extract → transform → load
        """
        # Calculate dynamic start/end at runtime
        end_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(hours=HOURS_BACK + LAG_HOURS)
        print(
            f"Fetching data from {start_time.isoformat()} to {end_time.isoformat()} UTC"
        )

        # Get DB session
        db_generator = get_sync_db()
        db = next(db_generator)

        # Instantiate ETL
        etl = FacilityTimeseriesETL(
            db=db,
            start_time=start_time,
            end_time=end_time,
            interval="1h",
            num_facility=BATCH_SIZE,
        )

        # 1️⃣ Extract
        print("🔹 Extracting facility timeseries data...")
        raw_data = etl.extract()
        print(f"✅ Extracted rows: {len(raw_data)}")

        # 2️⃣ Transform
        transformed = etl.transform(raw_data)
        print(f"✅ Transformed rows: {len(transformed)}")

        # 3️⃣ Load
        inserted, updated, skipped = etl.load(transformed, db, batch_size=BATCH_SIZE)
        print(
            f"✅ ETL completed. Inserted: {inserted}, Updated: {updated}, Skipped: {skipped}"
        )

    run_etl()


etl_dag = facility_timeseries_daily_pipeline()
