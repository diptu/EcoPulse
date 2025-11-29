import asyncio

from app.etl.run_etl import run_etl

if __name__ == "__main__":
    asyncio.run(run_etl())
