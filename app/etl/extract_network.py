# app/etl/extract_network.py
import logging
import os
from typing import Any, Dict, List, Optional

# --- CRITICAL CHANGE: Import serialization helper from transform ---
# This links the necessary conversion logic.

# -------------------------------
# Configuration / API Key Setup
# -------------------------------
API_KEY: Optional[str] = os.environ.get("OPENELECTRICITY_API_KEY")
try:
    if not API_KEY:
        from app.core.config import settings

        API_KEY = getattr(settings, "OPENELECTRICITY_API_KEY", None)
except ImportError:
    pass

# Make API_KEY available as env var (empty string if missing)
os.environ["OPENELECTRICITY_API_KEY"] = API_KEY or ""

# -------------------------------
# Imports
# -------------------------------
from openelectricity import OEClient
from openelectricity.client import APIError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.getLogger("openelectricity.client").setLevel(logging.ERROR)


# -------------------------------
# Network Timeseries Extract
# -------------------------------
from datetime import datetime, timedelta

from openelectricity.types import DataMetric


# -------------------------------
# Serialization Helper
# -------------------------------
def serialize_network_row(
    timestamp: datetime,
    metric: str,
    unit: str,
    name: str,
    fuel_group: str,
    value: float,
) -> Dict[str, Any]:
    """Serialize flattened network timeseries row for XCom."""
    return {
        "timestamp": timestamp.isoformat(),
        "metric": metric,
        "unit": unit,
        "name": name,
        "fuel_group": fuel_group,
        "value": value,
    }


def fetch_network_timeseries_single(
    client: OEClient,
    network_code: str,
    metric: DataMetric,
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Fetch network-level timeseries for a single metric.
    Returns a list of serialized JSON-safe dict rows.
    """
    print(f"  -> Fetching Network: {network_code} | Metric: {metric.value}")

    try:
        response = client.get_network_data(
            network_code=network_code,
            metrics=[metric],
            **params,
        )
    except APIError as e:
        status_code = getattr(e, "status_code", None) or (e.args[0] if e.args else None)

        if status_code == 403:
            print(f"⚠️ Skipping {network_code} | {metric.value} (**403 Forbidden**)")
            return []

        print(f"❌ API Error {status_code} for {network_code} | {metric.value}: {e}")
        return []

    flat_rows = []

    for series in response.data:
        metric_name = getattr(series, "metric", None)
        unit = getattr(series, "unit", None)

        for result in series.results:
            name = result.name
            fuel_group = getattr(result.columns, "fueltech_group", "N/A")

            for point in result.data:
                flat_rows.append(
                    serialize_network_row(
                        timestamp=point.timestamp,
                        metric=metric_name,
                        unit=unit,
                        name=name,
                        fuel_group=fuel_group,
                        value=point.value,
                    )
                )

    return flat_rows


def extract_network() -> List[Dict[str, Any]]:
    """
    Extract network-level timeseries (1h interval) for NEM + WEM across all core metrics.
    Returns JSON-ready flattened rows for XCom.
    """
    if not API_KEY:
        raise ValueError("❌ OPENELECTRICITY_API_KEY is missing!")

    # Config
    # networks = ["NEM", "WEM"]
    networks = ["NEM"]

    metrics = [
        DataMetric.POWER,
        # DataMetric.ENERGY,
        # DataMetric.EMISSIONS,
        # DataMetric.MARKET_VALUE,
        # DataMetric.RENEWABLE_PROPORTION,
        # DataMetric.STORAGE_BATTERY,
    ]

    # Past 24 hours
    now = datetime.utcnow()
    params = {
        "interval": "1h",
        "date_start": now - timedelta(days=1),
        "date_end": now,
        "secondary_grouping": "fueltech_group",
        "primary_grouping": "network_region",
    }

    all_rows: List[Dict[str, Any]] = []

    with OEClient() as client:
        for network in networks:
            for metric in metrics:
                rows = fetch_network_timeseries_single(
                    client=client,
                    network_code=network,
                    metric=metric,
                    params=params,
                )
                all_rows.extend(rows)

    print(f"✓ extract_network: {len(all_rows):,} rows collected")
    return all_rows
