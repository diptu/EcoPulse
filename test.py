# test.py
from __future__ import annotations

import os
import sys
from datetime import datetime

# -------------------------------
# Load API key from settings
# -------------------------------
try:
    from app.core.config import settings

    API_KEY = settings.OPENELECTRICITY_API_KEY
except ImportError:
    # Attempt to load from environment variable
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
from openelectricity import DataMetric, OEClient
from openelectricity.types import DataMetric

# Initialize with environment variables (recommended)
client = OEClient()

import pandas as pd

# Get latest curtailment data (omit date_end for latest)
response = client.get_facility_data(
    network_code="NEM",
    facility_code=["BAYSW1", "ERARING"],
    metrics=[DataMetric.POWER, DataMetric.EMISSIONS],
    interval="1h",
    date_start=datetime(2024, 1, 1),
    date_end=datetime(2024, 1, 2),
)

# Convert to DataFrame
data = []
for timeseries in response.data:
    for result in timeseries.results:
        # print("=============Result============")

        # print(json.dumps(result.model_dump(), indent=4, default=str))
        # print("=============Result============")
        # exit(0)
        name = result.name
        # region = name.split("_")[-1]  # Extract name from region

        # print(
        #     f"result.columns.unit_code:{result.columns.unit_code},\
        #        result.columns.fueltech_group:{result.columns.fueltech_group}\
        #         result.columns.network_region:{result.columns.network_region}"
        # )
        unit_code = result.columns.unit_code
        fueltech_group = result.columns.fueltech_group
        network_region = result.columns.network_region

        for data_point in result.data:
            # print(f"data_point:{data_point.model_dump_json}")
            data.append(
                {
                    # "name": name,  # name = metric_unit_code
                    # "region": region,
                    "unit_code": unit_code,
                    "fueltech_group": fueltech_group or None,
                    "network_region": network_region or None,
                    "timestamp": data_point.timestamp,
                    "value": data_point.value,
                    "metric": timeseries.metric,
                    "unit": timeseries.unit,
                }
            )

df = pd.DataFrame(data)
print(df.columns)
df.to_csv("output/DL_FACILITY_TIMESERIES_DOCUMENTATION_TEST.csv")
