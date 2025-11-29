"""
OpenElectricity Facility Metadata Fetcher

Fetches facility metadata from OpenElectricity API, filtering by network,
status, and fuel technology, and exports results to CSV.
"""

from __future__ import annotations

import csv
import os
import sys
from typing import Any, List

# -------------------------------
# Load API key from settings
# -------------------------------
try:
    from app.core.config import settings

    API_KEY = settings.OPENELECTRICITY_API_KEY
except ImportError:
    print(
        "⚠️ Using environment variable for API key (assuming OPENELECTRICITY_API_KEY is set)."
    )
    API_KEY = os.environ.get("OPENELECTRICITY_API_KEY")

if not API_KEY:
    print("\n❌ OPENELECTRICITY_API_KEY is missing!\n")
    sys.exit(1)

os.environ["OPENELECTRICITY_API_KEY"] = API_KEY

# -------------------------------
# OEClient Imports
# -------------------------------
from openelectricity.client import APIError, OEClient
from openelectricity.types import UnitFueltechType, UnitStatusType


# -------------------------------
# Helpers
# -------------------------------
def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def fetch_facilities_single(
    client: OEClient,
    network_id: str,
    status_ids: List[UnitStatusType],
    fueltech_id: UnitFueltechType,
) -> List[Any]:
    """Fetch facilities for a single network/fueltech combination with 403 skip."""
    print(f"  -> Fetching: {network_id} | {fueltech_id.value}")
    try:
        return client.get_facilities(
            network_id=[network_id],
            status_id=status_ids,
            fueltech_id=[fueltech_id],
        ).data
    except APIError as e:
        # APIError may not have status_code; fallback to args
        status_code = getattr(e, "status_code", None) or (e.args[0] if e.args else None)
        if status_code == 403:
            print(f"⚠️ Skipping {network_id} | {fueltech_id.value} (403 Forbidden)")
            return []
        # Log other errors but continue fetching next fueltech
        print(f"❌ API Error {status_code} for {network_id} | {fueltech_id.value}: {e}")
        return []


def save_facilities_to_csv(facilities: List[Any], output_file: str) -> None:
    """
    Save facility metadata to CSV using all available attributes dynamically.

    This function is updated to inspect the first record to generate a header
    with ALL fields, including nested ones like 'units' (which will output as a string).
    """
    if not facilities:
        print("⚠️ No facilities to save. Skipping CSV export.")
        return

    # 1. Determine all available columns (keys) from the first object
    # The 'vars()' function gets the internal dictionary of the facility object.
    all_keys = list(vars(facilities[0]).keys())

    ensure_dir(output_file)
    print(f"✅ Discovered {len(all_keys)} unique metadata fields.")

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # 2. Write the dynamic header row
        writer.writerow(all_keys)

        # 3. Write data rows
        for fac in facilities:
            # Get the internal dictionary of the current facility object
            fac_dict = vars(fac)

            # Use a list comprehension to pull values for the known keys
            row_values = [fac_dict.get(key) for key in all_keys]

            # Simple serialization: convert complex objects (like list of units) to string
            row_values = [
                str(val) if isinstance(val, (list, dict)) else val for val in row_values
            ]

            writer.writerow(row_values)


# -------------------------------
# Main
# -------------------------------
def main():
    print("🌏 Fetching OpenElectricity Facility Metadata...")
    output_csv = "./output/facilities_metadata.csv"
    all_facilities: List[Any] = []

    networks = ["NEM", "WEM"]
    status_ids = [UnitStatusType.OPERATING]
    fueltech_ids = [
        UnitFueltechType.SOLAR_UTILITY,
        UnitFueltechType.WIND,
        UnitFueltechType.SOLAR_ROOFTOP,
        UnitFueltechType.HYDRO,
        UnitFueltechType.BATTERY_DISCHARGING,
        UnitFueltechType.BATTERY_CHARGING,
        UnitFueltechType.COAL_BLACK,
        UnitFueltechType.COAL_BROWN,
        UnitFueltechType.GAS_CCGT,
        UnitFueltechType.GAS_OCGT,
        UnitFueltechType.DISTILLATE,
        UnitFueltechType.BIOENERGY_BIOMASS,
        UnitFueltechType.BIOENERGY_BIOGAS,
    ]

    with OEClient() as client:
        for network in networks:
            for fueltech in fueltech_ids:
                facilities = fetch_facilities_single(
                    client, network, status_ids, fueltech
                )
                all_facilities.extend(facilities)

    save_facilities_to_csv(all_facilities, output_csv)

    if all_facilities:
        print(
            f"✓ Facilities metadata saved successfully ({len(all_facilities)} records)."
        )
        print(f"📤 CSV Export: {output_csv}")
    else:
        print("✓ Completed run. No facility records were retrieved.")


# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    main()
