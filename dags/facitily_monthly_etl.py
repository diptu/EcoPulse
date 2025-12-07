# dags/etl_monthly.py
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task

# Import module dependencies
from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.facility import FacilityETL

# -------------------------------
## ⚙️ Settings & Configuration
# -------------------------------

# Access settings
BATCH_SIZE = settings.BATCH_SIZE
API_KEY: Optional[str] = getattr(settings, "OPENELECTRICITY_API_KEY", None)

# --- Pre-run Check ---
if not API_KEY:
    raise ValueError(
        "❌ OPENELECTRICITY_API_KEY is missing! Set it as an environment variable or in config."
    )

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Airflow Default Args ---
default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------
## 🧱 DAG Definition
# -------------------------------


@dag(
    dag_id="facility_monthly_pipeline",
    default_args=default_args,
    schedule="@monthly",
    catchup=False,
    tags=["opennem", "facilities", "monthly"],
)
def facility_monthly_pipeline():
    # -------------------------------
    # 1️⃣ Extract Task
    # -------------------------------
    @task()
    def extract() -> List[Dict[str, Any]]:
        logger.info("🔹 Extracting raw facilities from OpenElectricity API...")
        etl = FacilityETL()
        raw_data = etl.extract()
        # # FIX: Normalize timestamp values before XCom
        # for row in raw:
        #     ts = row.get("timestamp")
        #     if isinstance(ts, datetime):
        #         # Always provide explicit timezone to avoid Airflow serialization bug
        #         row["timestamp"] = ts.replace(tzinfo=None).isoformat()

        logger.info(f"✅ Extracted {len(raw_data)} facilities")
        return raw_data

    # -------------------------------
    # 2️⃣ Transform Task
    # -------------------------------
    @task()
    def transform(raw_facilities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info("🔹 Transforming facilities...")

        etl = FacilityETL()
        transformed = etl.transform(raw_facilities)

        # ✅ Ensure datetime objects are ISO strings for XCom
        for row in transformed:
            if "timestamp" in row and isinstance(row["timestamp"], datetime):
                row["timestamp"] = row["timestamp"].isoformat()

        logger.info(f"✅ Transformed {len(transformed)} facilities")
        return transformed

    # -------------------------------
    # 3️⃣ Load Task
    # -------------------------------
    @task()
    def load(transformed_facilities: List[Dict[str, Any]]):
        logger.info("💾 Loading facilities into DB...")
        db = next(get_sync_db())
        etl = FacilityETL()
        total_inserted = 0

        # Perform batched loading
        for i in range(0, len(transformed_facilities), BATCH_SIZE):
            batch = transformed_facilities[i : i + BATCH_SIZE]
            # Assumes etl.load handles the database session (db) and insertion
            etl.load(batch, db=db, batch_size=BATCH_SIZE)
            total_inserted += len(batch)

        logger.info(f"✅ ETL job completed. Total processed: {total_inserted}")

    # -------------------------------
    ## 🌊 Pipeline Flow
    # -------------------------------
    raw = extract()
    transformed = transform(raw)
    load(transformed)


# Instantiate the DAG object
etl_dag = facility_monthly_pipeline()
