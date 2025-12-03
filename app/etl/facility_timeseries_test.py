from datetime import datetime

import pandas as pd
from openelectricity import DataMetric, OEClient

# Initialize with environment variables (recommended)
client = OEClient()
# Get latest curtailment data (omit date_end for latest)
response = client.get_facility_data(
    network_code="NEM",
    facility_code=["TESLA_GERALDTON", "WINDHILL", "ALINTA_WWF", "JEERB", "SAPHWF1"],
    metrics=[DataMetric.POWER, DataMetric.EMISSIONS],
    interval="1h",
    date_start=datetime(2024, 1, 1),
    date_end=datetime(2024, 1, 2),
)
# Convert to DataFrame
data = []
for timeseries in response.data:
    for result in timeseries.results:
        region = result.name.split("_")[-1]  # Extract region from name
        for data_point in result.data:
            data.append(
                {
                    "timestamp": data_point.timestamp,
                    "region": region,
                    "metric": timeseries.metric,
                    "value": data_point.value,
                    "unit": timeseries.unit,
                }
            )

df = pd.DataFrame(data)

print(df.shape)
