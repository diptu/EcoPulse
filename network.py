"""
Unified OpenElectricity data fetcher & CSV exporter.
ASYNC and DL OPTIMIZED: Fetches 1h data across all networks and metrics
into a single, unified CSV file using asynchronous operations for efficiency.
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

# -------------------------------
# Load API key from settings
# -------------------------------
try:
    from app.core.config import settings

    API_KEY = settings.OPENELECTRICITY_API_KEY
except ImportError:
    # Fallback to environment variable
    API_KEY = os.environ.get("OPENELECTRICITY_API_KEY")

if not API_KEY:
    print(
        "\n❌ OPENELECTRICITY_API_KEY is missing! Set it as an environment variable or in config.\n"
    )
    sys.exit(1)

os.environ["OPENELECTRICITY_API_KEY"] = API_KEY

# ------------------------------------------------------------------------------
# OEClient Imports
# ------------------------------------------------------------------------------
from openelectricity import AsyncOEClient  # The dedicated async client
from openelectricity.types import DataMetric

# ------------------------------------------------------------------------------
# GLOBAL CONFIGURATION FOR ITERATION
# ------------------------------------------------------------------------------

NETWORK_CODES: List[str] = ["NEM", "WEM"]
METRICS: List[DataMetric] = [
    DataMetric.POWER,
    DataMetric.ENERGY,
    DataMetric.EMISSIONS,
    DataMetric.MARKET_VALUE,
    DataMetric.RENEWABLE_PROPORTION,
    DataMetric.STORAGE_BATTERY,
]

# ------------------------------------------------------------------------------
# Helpers (Unchanged)
# ------------------------------------------------------------------------------


def format_ts(ts: datetime) -> str:
    """Format timestamp in a standard form."""
    return ts.strftime("%Y-%m-%d %H:%M")


def ensure_dir(path: str):
    """Ensure directory exists before writing CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)


def custom_serializer(obj: Any) -> Any:
    """
    Custom serializer to handle non-JSON serializable objects
    returned by the OEClient (like datetime objects and custom SDK types).
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    if hasattr(obj, "value"):
        return obj.value
    return str(obj)


# ------------------------------------------------------------------------------
# UNIVERSAL TIMESERIES ITERATOR (Unchanged)
# ------------------------------------------------------------------------------


def iter_rows(data: Any, mode: str):
    """
    Generator that flattens nested timeseries data into a single CSV row format.
    """
    for series in data:
        if not hasattr(series, "metric") or not hasattr(series, "unit"):
            continue

        metric = series.metric
        unit = series.unit

        for result in series.results:
            name = result.name
            # Safely retrieve fueltech_group, defaulting to 'N/A' if not grouped
            fuel_group = getattr(result.columns, "fueltech_group", "N/A")

            for point in result.data:
                ts = format_ts(point.timestamp)
                value = point.value

                if mode == "network":
                    yield [ts, metric, unit, name, fuel_group, value]

                # Other modes (market, facility) omitted for this network-focused task


# ------------------------------------------------------------------------------
# ASYNC FETCH FUNCTION
# ------------------------------------------------------------------------------


async def fetch_network_data(
    client: AsyncOEClient, network_code: str, metrics: List[DataMetric], **params
) -> Any:
    """
    Fetches network data, logs details, and returns the raw response using async client.
    """

    # 1. Log API call details
    print(
        f"\n  -> Executing Fetch: Network={network_code} | Metrics={[m.value for m in metrics]}"
    )
    if params:
        param_list = [f"{k}={v}" for k, v in params.items()]
        print(f"  -> Filters Applied: {', '.join(param_list)}")

    # 2. Execute the API call and capture the response
    response_data = (
        await client.get_network_data(
            network_code=network_code, metrics=metrics, **params
        )
    ).data

    # 3. DYNAMIC COLUMN INSPECTION (For logging)
    base_columns = ["timestamp", "metric", "unit", "name", "value"]
    dynamic_columns = []

    if response_data and response_data[0].results:
        columns_metadata = response_data[0].results[0].columns
        for key, value in columns_metadata.__dict__.items():
            if value is not None or key in (
                "fueltech_group",
                "network_region",
                "unit_code",
            ):
                dynamic_columns.append(key)

    all_columns = base_columns + dynamic_columns
    print(f"  -> Expected Output Columns (CSV/Flattened): {', '.join(all_columns)}")

    # 4. Return the raw response data
    return response_data


# ------------------------------------------------------------------------------
# CSV WRITERS (Unchanged)
# ------------------------------------------------------------------------------


def save_csv(rows, header: List[str], output_file: str, append: bool = False):
    """Writes or appends data rows to a CSV file."""
    ensure_dir(output_file)
    mode = "a" if append and os.path.exists(output_file) else "w"

    with open(output_file, mode=mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if mode == "w":
            writer.writerow(header)

        writer.writerows(rows)


# ------------------------------------------------------------------------------
# ASYNC TASK WRAPPER (FIX for data association and logging)
# ------------------------------------------------------------------------------


async def fetch_and_process_task(
    client, network_code, metric, **params
) -> Dict[str, Union[str, DataMetric, Any]]:
    """
    Wrapper function to execute fetch_network_data, handling exceptions
    and returning the original parameters alongside the data/error.
    """
    try:
        network_data = await fetch_network_data(
            client, network_code=network_code, metrics=[metric], **params
        )
        return {"network_code": network_code, "metric": metric, "data": network_data}
    except Exception as e:
        return {"network_code": network_code, "metric": metric, "error": e}


# ------------------------------------------------------------------------------
# ASYNC MAIN WORKFLOW
# ------------------------------------------------------------------------------


async def main():
    print("🧠 Optimizing Data Fetch for Deep Learning Training (Async 1h Interval)...")

    # Define parameters for DL training set (3 years @ 1h interval)
    DAYS_OF_HISTORY = 1
    ONE_YEAR_AGO = datetime.now() - timedelta(days=DAYS_OF_HISTORY)
    NOW = datetime.now()
    INTERVAL = "1h"

    OUTPUT_FILE = "./output/DL_NETWORK_TIMESERIES_UNIFIED.csv"

    HEADER = [
        "timestamp",
        "metric",
        "unit",
        "name",
        "fuel_group",
        "value",
    ]

    all_rows: List[List[Any]] = []
    total_records = 0

    try:
        async with AsyncOEClient() as client:
            tasks = []

            # Prepare all concurrent tasks (Network x Metric = 12 tasks)
            for network_code in NETWORK_CODES:
                for metric in METRICS:
                    params = {
                        "interval": INTERVAL,
                        "date_start": ONE_YEAR_AGO,
                        "date_end": NOW,
                        "secondary_grouping": "fueltech_group",
                        "primary_grouping": "network_region",
                    }
                    task = fetch_and_process_task(
                        client, network_code, metric, **params
                    )
                    tasks.append(task)

            print(f"\n⚡ Initiating {len(tasks)} concurrent API calls...")

            # Run all tasks concurrently and wait for completion
            results = await asyncio.gather(*tasks)

            # Process results
            for result in results:
                network_code = result["network_code"]
                metric = result["metric"]

                if "error" in result:
                    print(
                        f"  ❌ Failed to fetch {network_code}/{metric.value}: {result['error']}"
                    )
                else:
                    network_data = result["data"]
                    current_rows = list(iter_rows(network_data, mode="network"))

                    if current_rows:
                        all_rows.extend(current_rows)
                        total_records += len(current_rows)

                        print(
                            f"  ✓ Fetched {network_code}/{metric.value}. Added {len(current_rows):,} rows."
                        )
                    else:
                        print(
                            f"  ⚠️ Fetched {network_code}/{metric.value}, but received 0 rows. Skipping."
                        )

            # 4. FINAL SAVE OPERATION
            print(f"\n\n💾 Writing {total_records:,} total records to unified file...")
            save_csv(all_rows, HEADER, OUTPUT_FILE, append=False)

            print(f"🎉 Success! Unified data file saved to: {OUTPUT_FILE}")

    except Exception as e:
        print(
            f"\n❌ A critical error occurred during client initialization or final write: {e}"
        )


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
