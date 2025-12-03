# app/etl/facility_timeseries_etl.py
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from openelectricity import OEClient
from openelectricity.types import DataMetric
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.etl.base import BaseETL
from app.models.facility import Facility
from app.models.facility_timeseries import FacilityTimeseries
from app.schemas.facility_timeseries import FacilityTimeseriesCreate

# -------------------------------
# Logging Configuration
# -------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FacilityTimeseriesETL(BaseETL):
    def __init__(
        self,
        db: Session,
        start_time: datetime = None,
        end_time: datetime = None,
        interval: str = "1h",
        num_facility: int = 20,  # batch size for facility_code
    ):
        super().__init__(start_time=start_time, end_time=end_time, interval=interval)
        self.db = db
        self.start_time = start_time or (datetime.utcnow() - timedelta(days=1))
        self.end_time = end_time or datetime.utcnow()
        self.interval = interval
        self.num_facility = num_facility

        self.network_code: str = "NEM"
        self.metrics: List[DataMetric] = [DataMetric.POWER, DataMetric.EMISSIONS]

        self.model_class = FacilityTimeseries
        self.schema_class = FacilityTimeseriesCreate

        # Fetch facility codes from DB
        self.facility_code: List[str] = self._get_facility_code()
        logger.info(f"✅ Initialized ETL for {len(self.facility_code)} facilities.")

    def _get_facility_code(self) -> List[str]:
        facilities = (
            self.db.query(Facility.code).order_by(Facility.id).limit(1000).all()
        )
        logger.info(f"✅ Fetched {len(facilities)} facility codes from DB.")
        return [f[0] for f in facilities]

    def extract(self) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []

        for i in range(0, len(self.facility_code), self.num_facility):
            batch_codes = self.facility_code[i : i + self.num_facility]
            params = {
                "interval": self.interval,
                "date_start": self.start_time.replace(tzinfo=None),
                "date_end": self.end_time.replace(tzinfo=None),
            }

            with OEClient() as client:
                try:
                    response = client.get_facility_data(
                        network_code=self.network_code,
                        facility_code=batch_codes,
                        metrics=self.metrics,
                        **params,
                    ).data

                    for series in response:
                        metric_val = (
                            series.metric.value
                            if hasattr(series.metric, "value")
                            else str(series.metric)
                        )
                        for result in series.results:
                            facility_code = result.name
                            unit_code = getattr(result.columns, "unit_code", "N/A")
                            fueltech_group = getattr(
                                result.columns, "fueltech_group", "N/A"
                            )
                            network_region = getattr(
                                result.columns, "network_region", "N/A"
                            )

                            for point in result.data:
                                all_rows.append(
                                    {
                                        "timestamp": point.timestamp.isoformat()
                                        if point.timestamp
                                        else None,  # ✅ JSON-serializable
                                        "metric": metric_val,
                                        "unit": series.unit,
                                        "facility_code": facility_code,
                                        "unit_code": unit_code,
                                        "fueltech_group": fueltech_group,
                                        "network_region": network_region,
                                        "value": point.value,
                                    }
                                )
                    logger.info(
                        f"✅ Fetched batch {i // self.num_facility + 1} "
                        f"({len(batch_codes)} facilities)"
                    )

                except Exception as e:
                    logger.error(
                        f"❌ Failed to fetch batch {i // self.num_facility + 1}: {e}"
                    )
                    code = getattr(e, "status_code", None)
                    if code == 400:
                        raise ValueError(
                            "400 Bad Request - Malformed parameters or chunk size too big."
                        )
                    elif code == 401:
                        raise PermissionError(
                            f"401 Unauthorized - Missing or invalid API key. Details: {e}"
                        )
                    elif code == 403:
                        raise PermissionError(
                            f"403 Forbidden - Insufficient permissions. Details: {e}"
                        )
                    elif code == 422:
                        raise ValueError(
                            f"422 Validation Error - Invalid input parameters. Details: {e}"
                        )
                    elif code == 500:
                        raise RuntimeError(
                            f"500 Internal Server Error - Server-side error. Details: {e}"
                        )
                    else:
                        raise RuntimeError(
                            f"❌ Unknown error fetching batch {i // self.num_facility + 1}: {e}"
                        )

        logger.info(f"✅ Total extracted rows: {len(all_rows)}")
        return all_rows

    def transform(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info(f"🔹 Transforming {len(rows)} rows...")
        # Convert timestamp strings back to datetime
        for row in rows:
            if "timestamp" in row and isinstance(row["timestamp"], str):
                row["timestamp"] = datetime.fromisoformat(row["timestamp"])

        transformed = super().transform(
            rows,
            schema=self.schema_class,
            csv_path="transformed_facility_timeseries.csv",
        )
        logger.info(f"✅ Transformed rows: {len(transformed)}")
        return transformed

    def load(
        self, rows: List[Dict[str, Any]], db: Session, batch_size: int = 100
    ) -> Tuple[int, int, int]:
        inserted_total, updated_total, skipped_total = 0, 0, 0

        logger.info(f"💾 Loading {len(rows)} rows into DB...")
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            stmt = insert(self.model_class).values(batch)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["timestamp", "facility_code"]
            )

            result = db.execute(stmt)
            db.commit()

            inserted_total += result.rowcount or 0
            skipped_total += len(batch) - (result.rowcount or 0)

        logger.info(
            f"💾 Load Summary: Inserted={inserted_total}, Skipped={skipped_total}"
        )
        return inserted_total, updated_total, skipped_total
