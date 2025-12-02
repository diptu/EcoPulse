# dags/etl_monthly.py
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from airflow.decorators import dag, task

from app.core.config import settings
from app.db.session import get_sync_db
from app.etl.extract import extract_facilities
from app.etl.load import load_facilities
from app.etl.transform import transform_facilities

BATCH_SIZE = settings.BATCH_SIZE
# -------------------------------
# Logging
# -------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# -------------------------------
# Configuration
# -------------------------------
START_DATE = datetime(2025, 1, 1)
default_args = {
    "owner": "data_engineer",
    "start_date": START_DATE,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Retrieve API key safely
API_KEY: Optional[str] = os.environ.get("OPENELECTRICITY_API_KEY")
try:
    if not API_KEY:
        from app.core.config import settings

        API_KEY = getattr(settings, "OPENELECTRICITY_API_KEY", None)
except ImportError:
    pass

# Make it available for tasks
os.environ["OPENELECTRICITY_API_KEY"] = API_KEY or ""

# --- Import your ETL logic ---
# Ensure these are installed in your Airflow environment
# from app.etl.extract import extract_facilities


# -------------------------------
# DAG Definition
# -------------------------------
@dag(
    dag_id="facility_pipeline_monthly",
    default_args=default_args,
    schedule="@monthly",
    catchup=False,
    tags=["opennem", "facilities"],
    doc_md=__doc__,
)
def facility_etl_monthly_pipeline():
    """
    Monthly ETL pipeline for OpenElectricity facility data.
    """

    @task()
    def extract():
        """
        Runs the extraction function.
        """

        logger.info("🔹 Extracting raw facilities from OpenElectricity API...")
        raw_data = extract_facilities()
        print(f"Fetched {len(raw_data)} facilities")
        return raw_data

    @task()
    def transform(raw_facilities: list) -> list:
        print("🔹 Transforming facilities...")
        transformed = transform_facilities(raw_facilities)
        print(f"✅ Transformed {len(transformed)} facilities")
        return transformed

    @task()
    def load(transformed):
        inserted_total = 0

        # 1. Manually create the generator object
        db_generator = get_sync_db()
        # 2. Extract the single yielded Session object
        # This executes the generator up to the 'yield db' line.
        db = next(db_generator)
        total = len(transformed)
        for i in range(0, total, BATCH_SIZE):
            batch = transformed[i : i + BATCH_SIZE]

            # Perform upsert
            load_facilities(batch, db, batch_size=BATCH_SIZE)
            inserted_total += len(batch)

        print(f"✅ ETL job completed. Total processed: {inserted_total}")

    # Task instantiation
    print("🌏 Starting ETL job...")

    # -------------------------------
    # 1. Extract
    # -------------------------------

    raw_data = extract()

    # -------------------------------
    # 2. Transform
    # -------------------------------

    transformed = transform(raw_data)

    # -------------------------------
    # 3. Load (Batch Upsert)
    # -------------------------------
    load(transformed)


# Instantiate DAG object for Airflow
etl_dag = facility_etl_monthly_pipeline()
