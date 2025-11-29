import asyncio

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from app.etl.run_etl import run_etl


@dag(
    schedule="@monthly",  # Change to @hourly for hourly DAG
    start_date=days_ago(1),
    catchup=False,
    tags=["ETL"],
)
def etl_dag():
    @task
    def execute_etl():
        asyncio.run(run_etl())

    execute_etl()


etl_dag = etl_dag()
