import os
from datetime import datetime

from airflow.decorators import dag, task


def show_var(key: str):
    """Print an environment variable with length + masked preview."""
    val = os.environ.get(key)

    if val is None:
        print(f"[MISSING] {key} is NOT SET")
        return None

    preview = val[:6] + "..." if len(val) > 6 else val
    print(f"[OK] {key} = '{preview}' (len={len(val)})")
    return val


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["debug", "env"],
)
def env_test():
    @task
    def check_envs():
        print("===== ENVIRONMENT VARIABLE VALIDATION =====")
        show_var("DATABASE_URL")
        show_var("SYNC_DATABASE_URL")
        show_var("OPENELECTRICITY_API_KEY")
        show_var("BATCH_SIZE")
        show_var("FACILITY_SIZE")
        print("===========================================")

    check_envs()


env_test_dag = env_test()
