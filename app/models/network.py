# app/models/network.py

from sqlalchemy import Column, DateTime, Float, String, UniqueConstraint

from app.models.base import BaseModel


class NetworkData(BaseModel):
    __tablename__ = "network_data"

    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)

    metric = Column(String, index=True, nullable=False)  # e.g., "power"
    unit = Column(String, nullable=False)  # e.g., "MW"

    name = Column(String, index=True, nullable=False)  # e.g., "power_NSW1|battery"
    fuel_group = Column(String, index=True, nullable=True)  # e.g., "battery"

    value = Column(Float, nullable=False)  # numeric measurement

    __table_args__ = (
        UniqueConstraint(
            "timestamp", "metric", "name", name="uq_timestamp_metric_name"
        ),
    )
