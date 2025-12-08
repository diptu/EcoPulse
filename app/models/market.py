# app/models/market.py

from sqlalchemy import Column, DateTime, Float, String, UniqueConstraint

from app.models.base import BaseModel


class MarketData(BaseModel):
    __tablename__ = "market_data"

    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)

    metric = Column(
        String,
        index=True,
        nullable=False,
    )  # e.g. "curtailment_solar_utility_energy"

    unit = Column(String, nullable=False)  # e.g. "MWh"

    region = Column(
        String,
        index=True,
        nullable=False,
    )  # e.g. "curtailment_solar_utility_energy_WEM"

    value = Column(Float, nullable=False)  # numeric values

    __table_args__ = (
        UniqueConstraint(
            "timestamp",
            "metric",
            "region",
            name="uq_timestamp_metric_region",
        ),
    )
