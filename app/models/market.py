# app/models/market.py

from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class MarketData(Base):
    __tablename__ = "market_data"

    id = Column(Integer, primary_key=True, index=True)

    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)

    metric = Column(
        String, index=True, nullable=False
    )  # e.g. "curtailment_solar_utility_energy"
    unit = Column(String, nullable=False)  # e.g. "MWh"

    region = Column(
        String, index=True, nullable=False
    )  # e.g. "curtailment_solar_utility_energy_WEM"

    value = Column(Float, nullable=False)  # numeric values

    created_at = Column(DateTime(timezone=True), server_default=func.now())
