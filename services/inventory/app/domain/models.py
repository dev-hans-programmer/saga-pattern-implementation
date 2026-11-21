from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel


class ReservationStatus(StrEnum):
    ACTIVE = "active"
    RELEASED = "released"


class Reservation(BaseModel):
    order_id: UUID
    sku: str
    quantity: int
    status: ReservationStatus = ReservationStatus.ACTIVE


class StockItem(BaseModel):
    sku: str
    available: int
