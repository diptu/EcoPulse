from __future__ import annotations

import asyncio
import csv
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from pytz import timezone

# -------------------------------
# Load API key from settings
# -------------------------------
try:
    from app.core.config import settings

    # Prioritize environment variable, then settings, then fail
    API_KEY = (
        os.environ.get("OPENELECTRICITY_API_KEY") or settings.OPENELECTRICITY_API_KEY
    )
except ImportError:
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
from openelectricity import AsyncOEClient
from openelectricity.types import DataMetric

# ------------------------------------------------------------------------------
# GLOBAL CONFIGURATION - MATCHING SUCCESSFUL BATCH
# ------------------------------------------------------------------------------

NETWORK_CODE: str = "NEM"
FACILITY_CODES: List[str] = ["BAYSW1", "ERARING"]
FACILITY_METRICS: List[DataMetric] = [DataMetric.POWER, DataMetric.EMISSIONS]
INTERVAL = "1h"
DAYS_OF_HISTORY = 1  # Fetch 24 hours of data

# ------------------------------------------------------------------------------
# Helpers (Updated to include unit_code, network_region)
# ------------------------------------------------------------------------------

from typing import Tuple

# Assume these constants are defined globally or passed in:
# DAYS_OF_HISTORY: int = 1
# LAG_HOURS: int = 3 # Adjusted to 3 hours for robust energy data fetching


def calculate_dynamic_time_range(
    hours_of_history: int, lag_hours: int, tz_name: str = "UTC"
) -> Tuple[datetime, datetime]:
    """
    Calculates the dynamic start and end datetime for API fetching,
    accounting for UTC time alignment, data publication lag, and desired history.

    The returned datetimes are naive (timezone-stripped) for compatibility
    with many API clients, but are calculated based on UTC time.

    Args:
        days_of_history: The total duration of history to fetch (in days).
        lag_hours: The number of hours to subtract from the current aligned time
                   to ensure the fetched data is reliably complete.
        tz_name: The timezone to use for the calculation (defaults to 'UTC').

    Returns:
        A tuple (date_start_naive, date_end_naive) of datetime objects.
    """

    # 1. Get current time and align to UTC
    NOW_UTC = datetime.now(timezone(tz_name))

    # 2. Align End Time: Down to the nearest hour (XX:00:00)
    ALIGNED_END_UTC = NOW_UTC.replace(minute=0, second=0, microsecond=0)

    # 3. Apply Lag: Subtract the defined lag hours to get the last reliably
    #    complete timestamp.
    DATE_END = ALIGNED_END_UTC - timedelta(hours=lag_hours)

    # 4. Calculate Start Time: Subtract the desired history duration.
    DATE_START = DATE_END - timedelta(days=hours_of_history)

    # 5. Strip timezone info for API compatibility
    DATE_START_NAIVE = DATE_START.replace(tzinfo=None)
    DATE_END_NAIVE = DATE_END.replace(tzinfo=None)

    return DATE_START_NAIVE, DATE_END_NAIVE


# Example usage (assuming DAYS_OF_HISTORY = 1 and LAG_HOURS = 3):
# START, END = calculate_dynamic_time_range(days_of_history=1, lag_hours=3)
# print(f"Start Time: {START.isoformat()} | End Time: {END.isoformat()}")
def format_ts(ts: datetime) -> str:
    """Format timestamp in ISO 8601 form. Returns Naive if input is naive."""
    return ts.isoformat()


def ensure_dir(path: str):
    """Ensure directory exists before writing CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)


def save_csv(
    rows: List[List[Any]], header: List[str], filename: str, append: bool = False
):
    """Saves the data rows to a CSV file."""
    ensure_dir(os.path.dirname(filename))
    mode = "a" if append else "w"
    with open(filename, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not append:
            writer.writerow(header)
        writer.writerows(rows)


def iter_rows(data: Any, mode: str):
    """
    Generator that flattens nested timeseries data into a single CSV row format.

    UPDATED: Now includes unit_code, fueltech_group, and network_region.
    """
    for series in data:
        if not hasattr(series, "metric") or not hasattr(series, "unit"):
            continue

        metric = series.metric
        unit = series.unit

        for result in series.results:
            facility_code = result.name

            # --- NEW METADATA EXTRACTION ---
            # Use getattr for fault tolerance against missing fields
            unit_code = getattr(result.columns, "unit_code", "N/A")
            fueltech_group = getattr(result.columns, "fueltech_group", "N/A")
            network_region = getattr(result.columns, "network_region", "N/A")
            # -------------------------------

            for point in result.data:
                ts = format_ts(point.timestamp)
                value = point.value

                if mode == "facility":
                    yield [
                        ts,
                        metric,
                        unit,
                        facility_code,
                        unit_code,  # NEW
                        fueltech_group,  # NEW/RETAINED (moved position)
                        network_region,  # NEW
                        value,
                    ]


# ------------------------------------------------------------------------------
# ASYNC FETCH & WRAPPERS (Unchanged)
# ------------------------------------------------------------------------------


async def fetch_facility_data(
    client: AsyncOEClient,
    network_code: str,
    facility_codes: List[str],
    metrics: List[DataMetric],
    **params,
) -> Any:
    """
    Fetches batched facility timeseries data.
    """
    metric_names = [m.value for m in metrics]
    print(
        f"\n  -> Executing Batched Fetch: Network={network_code} | Facilities={facility_codes} | Metrics={metric_names}"
    )
    if params:
        # Custom logging to show parameters
        param_list = [
            f"{k}={v}" if not isinstance(v, datetime) else f"{k}={v.isoformat()}"
            for k, v in params.items()
        ]
        print(f"  -> Filters Applied: {', '.join(param_list)}")

    response_data = (
        await client.get_facility_data(
            network_code=network_code,
            facility_code=facility_codes,
            metrics=metrics,
            **params,
        )
    ).data

    return response_data


async def fetch_and_process_task(
    client, network_code, facility_codes, metrics, **params
) -> Dict[str, Union[str, List[DataMetric], Any]]:
    """
    Wrapper function to execute fetch_facility_data, handling exceptions
    and returning the original parameters alongside the data/error.
    """
    try:
        network_data = await fetch_facility_data(
            client,
            network_code=network_code,
            facility_codes=facility_codes,
            metrics=metrics,
            **params,
        )
        return {
            "network_code": network_code,
            "metrics": metrics,
            "facility_codes": facility_codes,
            "data": network_data,
        }
    except Exception as e:
        return {
            "network_code": network_code,
            "metrics": metrics,
            "facility_codes": facility_codes,
            "error": e,
        }


# ------------------------------------------------------------------------------
# ASYNC MAIN WORKFLOW (Dynamic Time Calculation)
# ------------------------------------------------------------------------------


async def main():
    print(
        "🧠 Optimizing Data Fetch for Deep Learning Training (Dynamic Last 24 Hours)..."
    )

    # # 1. CRITICAL: Get current time and align to UTC
    # NOW_UTC = datetime.now(timezone("UTC"))

    # # 2. Define Lag and Time Range.
    # LAG_HOURS = 1

    # # Calculate End Time: Align the current time DOWN to the nearest hour (XX:00:00)
    # # and then subtract the lag.
    # ALIGNED_END_UTC = NOW_UTC.replace(minute=0, second=0, microsecond=0)
    # DATE_END = ALIGNED_END_UTC - timedelta(hours=LAG_HOURS)

    # # Calculate Start Time: Subtract the desired history days (24 hours)
    # DATE_START = DATE_END - timedelta(hours=DAYS_OF_HISTORY)

    # # Strip timezone info for API compatibility
    # DATE_START_NAIVE = DATE_START.replace(tzinfo=None)
    # DATE_END_NAIVE = DATE_END.replace(tzinfo=None)

    DATE_START_NAIVE, DATE_END_NAIVE = calculate_dynamic_time_range(
        hours_of_history=1, lag_hours=1
    )

    print(
        f"\n  📅 Fetching Period (UTC): {DATE_START_NAIVE.isoformat()} to {DATE_END_NAIVE.isoformat()}"
    )

    OUTPUT_FILE = "./output/DL_FACILITY_TIMESERIES_DYNAMIC_ENRICHED.csv"

    # --- UPDATED HEADER FOR NEW COLUMNS ---
    HEADER = [
        "timestamp",
        "metric",
        "unit",
        "facility_code",
        "unit_code",  # NEW
        "fueltech_group",
        "network_region",  # NEW
        "value",
    ]
    # --------------------------------------

    all_rows: List[List[Any]] = []
    total_records = 0
    failed_requests_count = 0
    total_tasks = 1

    try:
        async with AsyncOEClient() as client:
            # Prepare the SINGLE batched task
            params = {
                "interval": INTERVAL,
                "date_start": DATE_START_NAIVE,
                "date_end": DATE_END_NAIVE,
            }

            task = fetch_and_process_task(
                client, NETWORK_CODE, FACILITY_CODES, FACILITY_METRICS, **params
            )

            print(f"\n⚡ Initiating {total_tasks} batched API call(s)...")

            results = await asyncio.gather(task)

            # --- FAULT-TOLERANT PROCESSING BLOCK ---
            for result in results:
                if "error" in result:
                    error_message = str(result["error"])
                    print(
                        f"  ❌ FAILED REQUEST: {result['network_code']}/{result['facility_codes']}/{[m.value for m in result['metrics']]} -> {error_message}"
                    )
                    failed_requests_count += 1
                else:
                    network_data = result["data"]
                    current_rows = list(iter_rows(network_data, mode="facility"))

                    if current_rows:
                        all_rows.extend(current_rows)
                        total_records += len(current_rows)

                        print(
                            f"  ✅ SUCCESS: Fetched Batched Data. Added {len(current_rows):,} rows."
                        )
                    else:
                        print(
                            "  ⚠️ Fetched Batched Data, but received 0 rows. Check API documentation for latest facility data latency."
                        )

            # 4. FINAL SAVE OPERATION
            print(f"\n\n💾 Writing {total_records:,} total records to unified file...")
            save_csv(all_rows, HEADER, OUTPUT_FILE, append=False)

            print(f"🎉 Completion Summary! Total records saved: {total_records:,}.")

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
