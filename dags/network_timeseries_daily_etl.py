# dags/facility_timeseries_daily_etl.py
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task

# -------------------------------
# 🔑 Load settings and dependencies at module level
# -------------------------------
from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.network import NetworkETL

# Access settings
BATCH_SIZE = settings.BATCH_SIZE
FACILITY_SIZE = settings.FACILITY_SIZE
API_KEY: Optional[str] = getattr(settings, "OPENELECTRICITY_API_KEY", None)

# -------------------------------
# Configuration & Logging
# -------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

LAG_HOURS = 0
HOURS_BACK = 24

# -------------------------------
# Pre-run Check
# -------------------------------
if not API_KEY:
    raise ValueError(
        "❌ OPENELECTRICITY_API_KEY is missing! Set it as an environment variable or in config."
    )


# -------------------------------
# DAG DEFINITION
# -------------------------------
@dag(
    dag_id="network_timeseries_daily_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["opennem", "network_timeseries", "daily"],
)
def network_timeseries_daily_pipeline():
    # -------------------------------
    # 1️⃣ Extract Task
    # -------------------------------
    @task()
    def extract() -> List[Dict[str, Any]]:
        logger.info("🔹 Extracting raw facility timeseries from OpenElectricity API...")
        end_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(hours=HOURS_BACK + LAG_HOURS)
        logger.info(f"[EXTRACT] {start_time.isoformat()} → {end_time.isoformat()}")

        etl = NetworkETL(start_time=start_time, end_time=end_time, interval="1h")

        raw = etl.extract()

        # Convert all datetime objects to ISO strings BEFORE XCom storage
        for row in raw:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()

        logger.info(f"✅ Extracted {len(raw)} raw rows")
        return raw

    # -------------------------------
    # 2️⃣ Transform Task
    # -------------------------------
    @task()
    def transform(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info(f"🔹 Transforming {len(raw_data)} rows...")
        # db = next(get_sync_db())
        etl = NetworkETL(
            start_time=None,
            end_time=None,
            interval="1h",
        )

        transformed = etl.transform(raw_data)

        # ✅ Ensure datetime objects are ISO strings for XCom
        for row in transformed:
            if "timestamp" in row and isinstance(row["timestamp"], datetime):
                row["timestamp"] = row["timestamp"].isoformat()

        logger.info(f"✅ Transformed rows ready for XCom: {len(transformed)}")
        return transformed

    # -------------------------------
    # 3️⃣ Load Task
    # -------------------------------
    @task()
    def load(transformed_data: List[Dict[str, Any]]):
        db = next(get_sync_db())
        etl = NetworkETL(
            start_time=None,
            end_time=None,
            interval="1h",
        )

        # ✅ Convert ISO strings back to datetime before DB insert
        for row in transformed_data:
            if "timestamp" in row and isinstance(row["timestamp"], str):
                row["timestamp"] = datetime.fromisoformat(row["timestamp"])

        inserted, updated, skipped = etl.load(
            transformed_data, db, batch_size=BATCH_SIZE
        )

        logger.info(
            f"💾 Load completed — Inserted: {inserted}, Updated: {updated}, Skipped: {skipped}"
        )

    # -------------------------------
    # PIPELINE FLOW
    # -------------------------------
    logger.info("🌏 Starting Extraction job...")
    raw = extract()

    logger.info("🌏 Starting Transform job...")
    transformed = transform(raw)

    logger.info("🌏 Starting Load job...")
    load(transformed)

    logger.info("✅ ETL job completed.")


etl_dag = network_timeseries_daily_pipeline()
