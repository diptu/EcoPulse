"""
SYNC ETL Pipeline for OpenElectricity Data
- Extract: Fetch 1h network timeseries for all metrics + all networks
- Transform: Flatten SDK response into clean tabular rows
- Load: Write unified CSV dataset
"""

from __future__ import annotations

import csv
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List

# -------------------------------
# Load API key
# -------------------------------
try:
    from app.core.config import settings

    API_KEY = settings.OPENELECTRICITY_API_KEY
except ImportError:
    API_KEY = os.environ.get("OPENELECTRICITY_API_KEY")

if not API_KEY:
    print("❌ Missing OPENELECTRICITY_API_KEY")
    sys.exit(1)

os.environ["OPENELECTRICITY_API_KEY"] = API_KEY

# -------------------------------
# OEClient SYNC
# -------------------------------
from openelectricity import OEClient
from openelectricity.types import DataMetric

# -------------------------------
# Global Config
# -------------------------------

NETWORK_CODES = ["NEM", "WEM"]
METRICS = [
    DataMetric.POWER,
    DataMetric.ENERGY,
    DataMetric.EMISSIONS,
    DataMetric.MARKET_VALUE,
    DataMetric.RENEWABLE_PROPORTION,
    DataMetric.STORAGE_BATTERY,
]


# -------------------------------
# Helpers
# -------------------------------


def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def format_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M")


def iter_rows(data: Any, mode: str = "network"):
    """Flatten nested timeseries data into rows"""
    for series in data:
        metric = getattr(series, "metric", None)
        unit = getattr(series, "unit", None)

        if metric is None:
            continue

        for result in series.results:
            name = result.name
            fuel_group = getattr(result.columns, "fueltech_group", "N/A")

            for point in result.data:
                yield [
                    format_ts(point.timestamp),
                    metric,
                    unit,
                    name,
                    fuel_group,
                    point.value,
                ]


# ============================================================
# 1️⃣ EXTRACT — fetch SYNC network timeseries for each metric
# ============================================================


def extract_network_data(
    client: OEClient,
    network_code: str,
    metric: DataMetric,
    params: Dict[str, Any],
):
    """Extract raw data from OpenElectricity"""
    print(f"→ Extracting {network_code}/{metric.value}")

    response = client.get_network_data(
        network_code=network_code, metrics=[metric], **params
    )

    return {
        "network": network_code,
        "metric": metric,
        "data": response.data,
    }


# ============================================================
# 2️⃣ TRANSFORM — flatten nested structure → rows
# ============================================================


def transform(records: List[Dict[str, Any]]):
    """Convert raw OE response → CSV-ready row list."""
    all_rows = []
    total = 0

    for result in records:
        network_code = result["network"]
        metric = result["metric"]

        if "error" in result:
            print(f"❌ {network_code}/{metric.value}: {result['error']}")
            continue

        rows = list(iter_rows(result["data"], mode="network"))
        total += len(rows)

        print(f"✓ Transformed {network_code}/{metric.value}: {len(rows):,} rows")
        all_rows.extend(rows)

    return all_rows, total


# ============================================================
# 3️⃣ LOAD — write final unified CSV dataset
# ============================================================


def load(rows: List[List[Any]], output_file: str):
    """Write unified ETL output to CSV"""
    ensure_dir(output_file)

    header = [
        "timestamp",
        "metric",
        "unit",
        "name",
        "fuel_group",
        "value",
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"💾 Saved: {output_file}")


# ============================================================
# MAIN ETL ORCHESTRATOR
# ============================================================


def run_etl():
    print("🚀 Running SYNC ETL pipeline for OpenElectricity")

    DAYS_OF_HISTORY = 1
    START = datetime.now() - timedelta(days=DAYS_OF_HISTORY)
    END = datetime.now()
    INTERVAL = "1h"

    params = {
        "interval": INTERVAL,
        "date_start": START,
        "date_end": END,
        "secondary_grouping": "fueltech_group",
        "primary_grouping": "network_region",
    }

    OUTPUT_FILE = "./output/DL_NETWORK_TIMESERIES_UNIFIED.csv"

    extracted_records = []

    # ---------------------------
    # Extract
    # ---------------------------
    with OEClient(api_key=API_KEY) as client:
        for network in NETWORK_CODES:
            for metric in METRICS:
                try:
                    rec = extract_network_data(client, network, metric, params)
                    extracted_records.append(rec)
                except Exception as e:
                    extracted_records.append(
                        {"network": network, "metric": metric, "error": str(e)}
                    )

    # ---------------------------
    # Transform
    # ---------------------------
    rows, total = transform(extracted_records)

    print(f"📊 Total rows transformed: {total:,}")

    # ---------------------------
    # Load
    # ---------------------------
    load(rows, OUTPUT_FILE)

    print("🎉 ETL Completed Successfully!")


# Entrypoint
if __name__ == "__main__":
    run_etl()
