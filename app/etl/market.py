# app/etl/market_etl.py
from datetime import datetime, timedelta
from typing import Any, Dict, List

from openelectricity import OEClient
from openelectricity.types import MarketMetric

from app.etl.base import BaseETL
from app.models.market import MarketData
from app.schemas.market import MarketDataCreate


class MarketETL(BaseETL):
    def __init__(
        self,
        start_time: datetime = None,
        end_time: datetime = None,
        interval: str = "1h",
    ):
        super().__init__(start_time=start_time, end_time=end_time, interval=interval)
        self.start_time = start_time or (datetime.utcnow() - timedelta(days=1))
        self.end_time = end_time or datetime.utcnow()
        self.interval = interval

        self.networks: List[str] = ["NEM", "WEM"]
        self.metrics: List[MarketMetric] = [
            MarketMetric.CURTAILMENT,
            MarketMetric.CURTAILMENT_SOLAR_UTILITY,
            MarketMetric.CURTAILMENT_SOLAR_UTILITY_ENERGY,
            MarketMetric.CURTAILMENT_WIND_ENERGY,
            MarketMetric.CURTAILMENT_ENERGY,
            MarketMetric.CURTAILMENT_WIND,
            MarketMetric.DEMAND,
            MarketMetric.DEMAND_ENERGY,
            MarketMetric.PRICE,
        ]

        # 👈 Attributes for the runner
        self.model_class = MarketData
        self.schema_class = MarketDataCreate

    def extract(self) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []

        params = {
            "interval": self.interval,
            "date_start": self.start_time,
            "date_end": self.end_time,
            "primary_grouping": "network_region",
        }

        with OEClient() as client:
            for network in self.networks:
                for metric in self.metrics:
                    try:
                        response = client.get_market(
                            network_code=network,
                            metrics=[metric],
                            **params,
                        ).data

                        for series in response:
                            metric_val = (
                                series.metric.value
                                if hasattr(series.metric, "value")
                                else str(series.metric)
                            )

                            for region in series.results:
                                for point in region.data:
                                    all_rows.append(
                                        {
                                            "timestamp": point.timestamp,
                                            "metric": metric_val,
                                            "unit": series.unit,
                                            "region": region.name,
                                            "value": point.value,
                                        }
                                    )
                        print(f"  ✓ Fetched {network}/{metric_val}")
                    except Exception as e:
                        print(f"  ❌ Skipping {network}/{metric}: {e}")

        return all_rows

    def transform(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return super().transform(
            rows, schema=self.schema_class, csv_path="transformed_market.csv"
        )

    def load(self, rows: List[Dict[str, Any]], db, batch_size: int = 100):
        return super().load(
            rows,
            db,
            model_class=self.model_class,
            key_columns=["timestamp", "metric", "region"],
            data_columns=["unit", "value"],
            batch_size=batch_size,
        )
