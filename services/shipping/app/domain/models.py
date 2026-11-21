from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class ShipmentStatus(StrEnum):
    SCHEDULED = "scheduled"
    CANCELLED = "cancelled"


class Shipment(BaseModel):
    shipment_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    shipping_address: str
    status: ShipmentStatus = ShipmentStatus.SCHEDULED
