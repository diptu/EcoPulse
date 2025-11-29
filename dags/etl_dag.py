# dags/etl_dag.py
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task  # noqa: F401
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

API_CONECTION_ID = "opnenem"


default_args = {
    "owner": "astro",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="opennem_etl_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",  # run every hour
    catchup=False,
    tags=["opennem", "etl"],
) as dag:
    @task()
    def extract_data():
        
   

# default_args = {
#     "owner": "astro",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="open_electricity_etl",
#     default_args=default_args,
#     start_date=datetime(2025, 11, 29),
#     schedule_interval="@hourly",  # run every hour
#     catchup=False,
#     tags=["openelectricity", "etl"],
# ) as dag:
#     run_etl_task = PythonOperator(
#         task_id="run_openelectricity_etl", python_callable=run_etl_sync
#     )

#     run_etl_task
