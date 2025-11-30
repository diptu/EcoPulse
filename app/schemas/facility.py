from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel


class FacilityBase(BaseModel):
    name: str
    network_id: str
    network_region: Optional[str] = None
    description: Optional[str] = None
    npi_id: Optional[str] = None

    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Raw list of units
    units: Optional[List[Dict[str, Any]]] = None


class FacilityCreate(FacilityBase):
    pass


class FacilityUpdate(BaseModel):
    name: Optional[str] = None
    network_id: Optional[str] = None
    network_region: Optional[str] = None
    description: Optional[str] = None
    npi_id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    units: Optional[List[Dict[str, Any]]] = None


class FacilityRead(FacilityBase):
    id: UUID

    class Config:
        orm_mode = True
