# app/schemas/facility_feature.py
from datetime import datetime
from typing import List, Optional

# Assuming Pydantic v2 for type hints and modern features
from pydantic import BaseModel, Field


class FacilityFeatureBase(BaseModel):
    """
    Base schema representing the flattened and feature-engineered facility data.
    These fields are ready for ingestion by a Deep Learning model.
    """

    # --- Primary Identifiers and Categorical Features ---
    code: str = Field(..., description="Unique facility code.")
    name: str = Field(..., description="Official facility name.")
    network_id: Optional[str] = Field(
        None, description="The electricity market (NEM/WEM)."
    )
    network_region: Optional[str] = Field(
        None, description="The regional code (NSW1, SA1, etc.)."
    )

    # --- Numerical/Continuous Features (Flattened from 'units' and 'location') ---
    latitude: Optional[float] = Field(
        None, description="Geographical latitude of the facility."
    )
    longitude: Optional[float] = Field(
        None, description="Geographical longitude of the facility."
    )

    total_capacity_registered_mw: Optional[float] = Field(
        None, description="Sum of registered capacity across all units (MW)."
    )
    total_capacity_maximum_mw: Optional[float] = Field(
        None, description="Sum of maximum operating capacity across all units (MW)."
    )

    # --- Aggregated Categorical Features (for Multi-Hot Encoding) ---
    # Using Union[List[str], None] to handle potential empty lists or nulls from the JSONB column
    fueltechs_list: Optional[List[str]] = Field(
        None,
        description="List of unique fuel technologies used by all units at the facility.",
    )

    # --- Aggregated Boolean Flags ---
    is_operating: Optional[bool] = Field(
        None, description="True if at least one unit is in 'OPERATING' status."
    )
    is_storage: Optional[bool] = Field(
        None, description="True if any unit is a battery (charging/discharging)."
    )

    # --- Audit Fields ---
    source_description: Optional[str] = Field(
        None, description="Short snippet from the original API description."
    )
    source_npi_id: Optional[str] = Field(None, description="Original NPI ID.")
    source_created_at: Optional[datetime] = Field(
        None, description="Original API record creation timestamp."
    )
    source_updated_at: Optional[datetime] = Field(
        None, description="Original API record update timestamp."
    )


class FacilityFeatureCreate(FacilityFeatureBase):
    """
    Schema used for creating a new facility record in the feature store.
    Inherits all feature fields.
    """

    pass


class FacilityFeatureRead(FacilityFeatureBase):
    """
    Schema used for reading data back from the feature store (for API output).
    Includes database-managed fields like primary key and maintenance timestamps.
    """

    id: int = Field(..., description="Database primary key (auto-generated).")

    # Database maintenance timestamps
    created_at: datetime = Field(..., description="Feature store creation timestamp.")
    updated_at: datetime = Field(
        ..., description="Feature store last update timestamp."
    )

    class Config:
        # Pydantic setting to allow mapping fields from SQLAlchemy ORM objects
        from_attributes = True
