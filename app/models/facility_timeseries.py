# app/models/facility_timeseries.py

from sqlalchemy import Column, DateTime, Float, String, UniqueConstraint

from app.models.base import BaseModel


class FacilityTimeseries(BaseModel):
    __tablename__ = "facility_timeseries"

    timestamp = Column(
        DateTime(timezone=True), nullable=False, index=True
    )  # local timestamp
    metric = Column(String, nullable=False, index=True)
    unit = Column(String, nullable=False)
    facility_code = Column(String, nullable=False, index=True)
    unit_code = Column(String, nullable=True)
    fueltech_group = Column(String, nullable=True)
    network_region = Column(String, nullable=True)
    value = Column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "timestamp",
            "facility_code",
            name="uq_timestamp_facility_code",
        ),
    )
