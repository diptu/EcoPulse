"""
Unified OpenElectricity data fetcher & CSV exporter.
Now using a single universal `iter_rows()` for all timeseries types.
"""

from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Any, List

from openelectricity import OEClient

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def format_ts(ts: datetime) -> str:
    """Format timestamp in a standard form."""
    return ts.strftime("%Y-%m-%d %H:%M")


def ensure_dir(path: str):
    """Ensure directory exists before writing CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)


# ------------------------------------------------------------------------------
# UNIVERSAL TIMESERIES ITERATOR (your version)
# ------------------------------------------------------------------------------


def iter_rows(data: Any, mode: str):
    """
    Universal row iterator for:
      - network
      - market
      - facility

    mode: "network" | "market" | "facility"
    """
    for series in data:
        metric = series.metric
        unit = series.unit

        for result in series.results:
            name = result.name
            fuel_group = getattr(result.columns, "fueltech_group", "N/A")

            for point in result.data:
                ts = format_ts(point.timestamp)
                value = point.value

                if mode == "network":
                    yield [ts, metric, unit, name, fuel_group, value]

                elif mode == "market":
                    yield [ts, metric, unit, name, value]

                elif mode == "facility":
                    yield [ts, metric, unit, name, fuel_group, value]

                else:
                    raise ValueError(f"Unknown mode: {mode}")


# ------------------------------------------------------------------------------
# FETCH FUNCTIONS
# ------------------------------------------------------------------------------


def fetch_network_data(client: OEClient, **params) -> Any:
    return client.get_network(**params).data


def fetch_market_data(client: OEClient, **params) -> Any:
    return client.get_markets(**params).data


def fetch_facility_timeseries(client: OEClient, **params) -> Any:
    return client.get_facility_data(**params).data


# ------------------------------------------------------------------------------
# CSV WRITERS
# ------------------------------------------------------------------------------


def save_csv(rows, header: List[str], output_file: str):
    ensure_dir(output_file)
    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def save_facilities_to_csv(facilities: Any, output_file: str):
    """Save facility metadata to CSV."""
    ensure_dir(output_file)

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["code", "name", "fueltech", "status", "network", "region"])

        for f in facilities:
            writer.writerow(
                [
                    f.code,
                    f.name,
                    getattr(f, "fueltech", "N/A"),
                    getattr(f, "status", "N/A"),
                    getattr(f, "network", "N/A"),
                    getattr(f, "network_region", "N/A"),
                ]
            )


# ------------------------------------------------------------------------------
# MAIN WORKFLOW
# ------------------------------------------------------------------------------


def main():
    print("🌏 Fetching OpenElectricity Data...")

    with OEClient() as client:
        # ----------------------------------------------------
        # 2) NETWORK TIMESERIES
        # ----------------------------------------------------
        network_data = fetch_network_data(
            client,
            network_id=["NEM"],
            metric_id=["power"],
            fueltech_id=["battery"],
        )

        save_csv(
            iter_rows(network_data, mode="network"),
            ["timestamp", "metric", "unit", "region", "fuel_group", "value"],
            "./output/network_timeseries.csv",
        )

        print("✓ Network timeseries saved")

        # ----------------------------------------------------
        # 3) MARKET TIMESERIES
        # ----------------------------------------------------
        market_data = fetch_market_data(
            client,
            market_id=["NEM"],
            metric_id=["demand"],
        )

        save_csv(
            iter_rows(market_data, mode="market"),
            ["timestamp", "metric", "unit", "region", "value"],
            "./output/market_timeseries.csv",
        )

        print("✓ Market timeseries saved")

        # ----------------------------------------------------
        # 4) FACILITY TIMESERIES
        # ----------------------------------------------------
        facility_data = fetch_facility_timeseries(
            client,
            facility_code=["LKBANNA1"],
            metric_id=["energy"],
        )

        save_csv(
            iter_rows(facility_data, mode="facility"),
            ["timestamp", "metric", "unit", "facility_code", "fuel_group", "value"],
            "./output/facility_timeseries.csv",
        )

        print("✓ Facility timeseries saved")

    print("🎉 All processing complete!")


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
