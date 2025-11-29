"""
Unified OpenElectricity Curtailment data fetcher & CSV exporter.
ASYNC and DL OPTIMIZED: Fetches 1d curtailment data across all networks and
metrics into a single, unified CSV file using asynchronous operations for efficiency.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from network import save_csv

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
from openelectricity import AsyncOEClient

# CRITICAL: Use MarketMetric for curtailment metrics as shown in docs
from openelectricity.types import MarketMetric

# ------------------------------------------------------------------------------
# GLOBAL CONFIGURATION FOR ITERATION (UPDATED FOR CURTAILMENT DATA)
# ------------------------------------------------------------------------------

NETWORK_CODES: List[str] = ["NEM", "WEM"]
# --- NEW CURTAILMENT METRICS ---
CURTAILMENT_METRICS: List[MarketMetric] = [
    MarketMetric.CURTAILMENT_SOLAR_UTILITY_ENERGY,
    MarketMetric.CURTAILMENT_WIND_ENERGY,
    MarketMetric.CURTAILMENT_ENERGY,
]

# ------------------------------------------------------------------------------
# Helpers (Modified iter_rows)
# ------------------------------------------------------------------------------


def format_ts(ts: datetime) -> str:
    """Format timestamp in a standard form."""
    return ts.strftime("%Y-%m-%d %H:%M")


def ensure_dir(path: str):
    """Ensure directory exists before writing CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)


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
            # Name will typically be the region code (e.g., 'NSW1', 'VIC1', etc.)
            name = result.name

            for point in result.data:
                ts = format_ts(point.timestamp)
                value = point.value

                # For curtailment data, we need timestamp, metric, unit, region, value
                if mode == "curtailment":
                    yield [ts, metric, unit, name, value]

                # Other modes (network, market) omitted for this specific task


# ------------------------------------------------------------------------------
# ASYNC FETCH FUNCTION (USING get_market)
# ------------------------------------------------------------------------------


async def fetch_curtailment_data(  # Function name changed
    client: AsyncOEClient, network_code: str, metrics: List[MarketMetric], **params
) -> Any:
    """
    Fetches market (curtailment) data via client.get_market, logs details,
    and returns the raw response using async client.
    """

    print(
        f"\n  -> Executing Fetch: Network={network_code} | Metrics={[m.value for m in metrics]}"
    )
    if params:
        param_list = [f"{k}={v}" for k, v in params.items()]
        print(f"  -> Filters Applied: {', '.join(param_list)}")

    # --- CRITICAL CHANGE: Use client.get_market() for curtailment data ---
    response_data = (
        await client.get_market(network_code=network_code, metrics=metrics, **params)
    ).data
    # ----------------------------------------------------------------------

    # Dynamic column inspection logic omitted for simplicity,
    # as the output structure is consistent with 'market' data.

    return response_data


# ------------------------------------------------------------------------------
# ASYNC TASK WRAPPER (Modified to use fetch_curtailment_data)
# ------------------------------------------------------------------------------


async def fetch_and_process_task(
    client, network_code, metric, **params
) -> Dict[str, Union[str, MarketMetric, Any]]:
    """
    Wrapper function to execute fetch_curtailment_data, handling exceptions
    and returning the original parameters alongside the data/error.
    """
    try:
        network_data = await fetch_curtailment_data(  # <-- Use curtailment fetcher
            client, network_code=network_code, metrics=[metric], **params
        )
        return {"network_code": network_code, "metric": metric, "data": network_data}
    except Exception as e:
        return {"network_code": network_code, "metric": metric, "error": e}


# ------------------------------------------------------------------------------
# ASYNC MAIN WORKFLOW (UPDATED FOR CURTAILMENT DATA)
# ------------------------------------------------------------------------------


async def main():
    print(
        "🧠 Optimizing Data Fetch for Deep Learning Training (Async 1d CURTAILMENT Data)..."
    )

    # Define parameters for DL training set (3 years @ 1d interval)
    DAYS_OF_HISTORY = 1
    ONE_YEAR_AGO = datetime.now() - timedelta(days=DAYS_OF_HISTORY)
    NOW = datetime.now()
    # --- CRITICAL CHANGE: Interval set to 1d (Daily Totals) ---
    INTERVAL = "1h"

    OUTPUT_FILE = "./output/DL_CURTAILMENT_TIMESERIES_UNIFIED.csv"

    # --- UPDATED HEADER FOR CURTAILMENT DATA ---
    HEADER = [
        "timestamp",
        "metric",
        "unit",
        "region",
        "value",
    ]
    # -------------------------------------------

    all_rows: List[List[Any]] = []
    total_records = 0

    try:
        async with AsyncOEClient() as client:
            tasks = []

            # Prepare all concurrent tasks (Network x Curtailment Metric)
            for network_code in NETWORK_CODES:
                for metric in CURTAILMENT_METRICS:  # <-- Use CURTAILMENT_METRICS
                    params = {
                        "interval": INTERVAL,
                        "date_start": ONE_YEAR_AGO,
                        "date_end": NOW,
                        "primary_grouping": "network_region",
                        # Secondary grouping is typically not relevant for total curtailment
                    }
                    task = fetch_and_process_task(
                        client, network_code, metric, **params
                    )
                    tasks.append(task)

            print(f"\n⚡ Initiating {len(tasks)} concurrent API calls...")

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

                    # --- CRITICAL CHANGE: Use mode="curtailment" ---
                    current_rows = list(iter_rows(network_data, mode="curtailment"))

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

            print(f"🎉 Success! Unified curtailment data file saved to: {OUTPUT_FILE}")

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
