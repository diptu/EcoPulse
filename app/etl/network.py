# app/etl/network_etl.py
from datetime import datetime, timedelta

from openelectricity import OEClient
from openelectricity.types import DataMetric

from app.etl.base import BaseETL
from app.models.network import NetworkData
from app.schemas.network import NetworkDataCreate


class NetworkETL(BaseETL):
    def __init__(
        self,
        start_time: datetime = None,
        end_time: datetime = None,
        interval="1h",
    ):
        super().__init__(start_time, end_time, interval)
        self.start_time = start_time or (datetime.utcnow() - timedelta(days=1))
        self.end_time = end_time or datetime.utcnow()
        self.interval = interval
        self.networks = ["NEM", "WEM"]
        self.metrics = [DataMetric.POWER]

        # 👈 Add these attributes for the runner
        self.model_class = NetworkData
        self.schema_class = NetworkDataCreate

    def extract(self):
        all_rows = []
        params = {
            "interval": self.interval,
            "date_start": self.start_time,
            "date_end": self.end_time,
            "primary_grouping": "network_region",
            "secondary_grouping": "fueltech_group",
        }
        with OEClient() as client:
            for network in self.networks:
                for metric in self.metrics:
                    try:
                        response = client.get_network_data(
                            network_code=network, metrics=[metric], **params
                        )
                        for series in response.data:
                            metric_name = getattr(series, "metric", None)
                            unit = getattr(series, "unit", None)
                            for result in series.results:
                                name = result.name
                                fuel_group = getattr(
                                    result.columns, "fueltech_group", "N/A"
                                )
                                for point in result.data:
                                    all_rows.append(
                                        {
                                            "timestamp": point.timestamp,
                                            "metric": metric_name,
                                            "unit": unit,
                                            "name": name,
                                            "fuel_group": fuel_group,
                                            "value": point.value,
                                        }
                                    )
                    except Exception as e:
                        print(f"Skipping {network}|{metric.value}: {e}")
        return all_rows

    def transform(self, rows):
        return super().transform(
            rows, schema=self.schema_class, csv_path="transformed_network.csv"
        )

    def load(self, rows, db, batch_size=200):
        return super().load(
            rows,
            db,
            model_class=self.model_class,
            key_columns=["timestamp", "metric", "name"],
            data_columns=["unit", "fuel_group", "value"],
            batch_size=batch_size,
        )
