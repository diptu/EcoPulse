from __future__ import annotations

from sqlalchemy import Column, Float, String, Text
from sqlalchemy.dialects.postgresql import JSONB

from app.models.base import BaseModel


class Facility(BaseModel):
    """
    Flattened Facility model where all FacilityUnit fields
    are aggregated into a single row.

    Aggregates:
      - capacities → summed
      - statuses/fueltech → primary or combined
      - date ranges → min/max
    """

    __tablename__ = "facilities"
    code = Column(String, nullable=False)
    name = Column(String, nullable=False)
    network_id = Column(String, nullable=False)
    network_region = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    npi_id = Column(String, nullable=True)

    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    # 👇 NEW: store FacilityUnit list as raw JSON
    units = Column(JSONB, nullable=True)
