# app/schemas/network.py
from datetime import datetime

from pydantic import BaseModel


class NetworkDataBase(BaseModel):
    timestamp: datetime
    metric: str
    unit: str
    name: str
    fuel_group: str | None = None
    value: float


class NetworkDataCreate(NetworkDataBase):
    pass


class NetworkDataRead(NetworkDataBase):
    id: int
    created_at: datetime

    class Config:
        orm_mode = True
