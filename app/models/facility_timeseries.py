# app/models/facility_timeseries.py
from sqlalchemy import Column, DateTime, Float, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class FacilityTimeseries(Base):
    __tablename__ = "facility_timeseries"

    id = Column(String, primary_key=True, index=True)  # UUID recommended
    timestamp = Column(DateTime, nullable=False, index=True)
    metric = Column(String, nullable=False, index=True)
    unit = Column(String, nullable=False)
    facility_code = Column(String, nullable=False, index=True)
    unit_code = Column(String, nullable=True)
    fueltech_group = Column(String, nullable=True)
    network_region = Column(String, nullable=True)
    value = Column(Float, nullable=False)
