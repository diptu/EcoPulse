# dags/etl_monthly.py
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task
from airflow.models import Variable

from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.facility import FacilityETL

# -------------------------------
# ⚙️ Settings & Configuration
# -------------------------------

BATCH_SIZE = settings.BATCH_SIZE
API_KEY: Optional[str] = getattr(settings, "OPENELECTRICITY_API_KEY", None)

if not API_KEY:
    raise ValueError(
        "❌ OPENELECTRICITY_API_KEY is missing! Set it as an environment variable or in config."
    )

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------
# 🧱 DAG Definition
# -------------------------------


@dag(
    dag_id="facility_monthly_pipeline",
    default_args=default_args,
    schedule="@monthly",
    catchup=False,
    tags=["opennem", "facilities", "monthly"],
)
def facility_monthly_pipeline():
    @task()
    def extract() -> List[Dict[str, Any]]:
        logger.info("🔹 Extracting raw facilities from OpenElectricity API...")
        etl = FacilityETL()
        raw_data = etl.extract()
        logger.info(f"✅ Extracted {len(raw_data)} facilities")
        return raw_data

    @task()
    def transform(raw_facilities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info("🔹 Transforming facilities...")
        etl = FacilityETL()
        transformed = etl.transform(raw_facilities)

        # Convert datetime to ISO strings for XCom
        for row in transformed:
            if "timestamp" in row and isinstance(row["timestamp"], datetime):
                row["timestamp"] = row["timestamp"].isoformat()

        logger.info(f"✅ Transformed {len(transformed)} facilities")
        return transformed

    @task()
    def load(transformed_facilities: List[Dict[str, Any]]):
        logger.info("💾 Loading facilities into DB...")

        # ✅ Use Astro deployment environment variable
        sync_url = (
            os.environ.get("SYNC_DATABASE_URL")
            or getattr(settings, "SYNC_DATABASE_URL", None)
            or Variable.get("SYNC_DATABASE_URL", default_var=None)
        )
        if not sync_url:
            raise RuntimeError(
                "SYNC_DATABASE_URL missing! Set it as an environment variable in Astro."
            )

        # Temporarily override settings for DAG runtime
        settings.SYNC_DATABASE_URL = sync_url

        db = next(get_sync_db())
        etl = FacilityETL()
        total_inserted = 0

        for i in range(0, len(transformed_facilities), BATCH_SIZE):
            batch = transformed_facilities[i : i + BATCH_SIZE]
            etl.load(batch, db=db, batch_size=BATCH_SIZE)
            total_inserted += len(batch)

        logger.info(f"✅ ETL job completed. Total processed: {total_inserted}")

    # -------------------------------
    # 🌊 Pipeline Flow
    # -------------------------------

    raw = extract()
    transformed = transform(raw)
    load(transformed)


# Instantiate the DAG (only once)
etl_dag = facility_monthly_pipeline()
etl_dag = facility_monthly_pipeline()
