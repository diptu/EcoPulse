# app/schemas/market.py

from datetime import datetime

from pydantic import BaseModel


class MarketDataBase(BaseModel):
    timestamp: datetime
    metric: str
    unit: str
    region: str
    value: float


class MarketDataCreate(MarketDataBase):
    pass


class MarketDataRead(MarketDataBase):
    id: int
    created_at: datetime

    class Config:
        orm_mode = True
