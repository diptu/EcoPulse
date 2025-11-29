# app/models/network.py

from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class NetworkData(Base):
    __tablename__ = "network_data"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)

    metric = Column(String, index=True, nullable=False)  # e.g., "power"
    unit = Column(String, nullable=False)  # e.g., "MW"

    name = Column(String, index=True, nullable=False)  # e.g., "power_NSW1|battery"
    fuel_group = Column(String, index=True, nullable=True)  # e.g., "battery"

    value = Column(Float, nullable=False)  # numeric measurement

    created_at = Column(DateTime(timezone=True), server_default=func.now())
