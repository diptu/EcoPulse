# app/schemas/facility_timeseries.py
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class FacilityTimeseriesBase(BaseModel):
    timestamp: datetime
    metric: str
    unit: str
    facility_code: str
    unit_code: Optional[str] = None
    fueltech_group: Optional[str] = None
    network_region: Optional[str] = None
    value: float


class FacilityTimeseriesCreate(FacilityTimeseriesBase):
    pass


class FacilityTimeseriesRead(FacilityTimeseriesBase):
    id: str

    class Config:
        orm_mode = True
