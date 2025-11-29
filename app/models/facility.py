# app/models/facility.py

# app/models/facility_feature.py
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, func
from sqlalchemy.dialects.postgresql import (
    JSONB,  # Use JSONB for better performance if using Postgres
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Facility(Base):
    """
    SQLAlchemy model optimized for ML feature storage.

    Flattens nested data and converts core geographical/capacity metrics
    into numerical columns ready for normalization and direct model input.
    """

    __tablename__ = "facility_features"

    # --- Primary Identifiers ---
    # Use original API code as a strong identifier
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, index=True, nullable=False, unique=True)

    # --- Extracted Categorical Features (for Embedding/One-Hot Encoding) ---
    name = Column(String, nullable=False)
    network_id = Column(String, nullable=True)  # NEM, WEM
    network_region = Column(String, nullable=True)  # NSW1, QLD1, SA1, etc.

    # --- Extracted Numerical Features (Flattened from 'units') ---
    # Aggregate Capacity: Sum of capacity_registered across all units
    total_capacity_registered_mw = Column(Float, nullable=True)
    # Aggregate Maximum Capacity: Sum of capacity_maximum across all units
    total_capacity_maximum_mw = Column(Float, nullable=True)

    # --- Extracted Geographical Features (Converted from 'location' string) ---
    # Lat/Lng are critical for spatial models or weather feature aggregation
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    # --- Unit-Level Feature Aggregation (for Multi-Feature Models) ---
    # One-Hot Encoding preparation: List of all fuel technologies used at the facility
    # If using PostgreSQL, ARRAY(String) is ideal. Otherwise, JSONB.
    # We will use JSONB as a general representation.
    fueltechs_list = Column(
        JSONB, nullable=True
    )  # E.g., ['solar_utility', 'battery_discharging']

    # --- Status and Type Aggregation ---
    is_operating = Column(
        Boolean, nullable=True
    )  # True if any unit's status is 'OPERATING'
    is_storage = Column(
        Boolean, nullable=True
    )  # True if any unit is BATTERY_CHARGING/DISCHARGING

    # --- Audit Fields (For ETL tracking) ---
    source_description = Column(
        String, nullable=True
    )  # A short text snippet from the original description
    source_npi_id = Column(String, nullable=True)  # Original NPI ID
    source_created_at = Column(DateTime, nullable=True)
    source_updated_at = Column(DateTime, nullable=True)

    # --- Database Maintenance ---
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
