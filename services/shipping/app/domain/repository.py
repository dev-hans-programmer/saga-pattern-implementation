import asyncio
from uuid import UUID

from .models import Shipment, ShipmentStatus


class ShippingRepository:
    def __init__(self) -> None:
        self._shipments: dict[UUID, Shipment] = {}
        self._lock = asyncio.Lock()

    async def schedule(self, order_id: UUID, shipping_address: str) -> Shipment:
        async with self._lock:
            existing = self._shipments.get(order_id)
            if existing:
                return existing.model_copy(deep=True)
            shipment = Shipment(order_id=order_id, shipping_address=shipping_address)
            self._shipments[order_id] = shipment
            return shipment.model_copy(deep=True)

    async def cancel(self, order_id: UUID) -> Shipment:
        async with self._lock:
            shipment = self._shipments.get(order_id)
            if shipment is None:
                raise ValueError("shipment does not exist")
            shipment.status = ShipmentStatus.CANCELLED
            return shipment.model_copy(deep=True)

    async def list(self) -> list[Shipment]:
        async with self._lock:
            return [shipment.model_copy(deep=True) for shipment in self._shipments.values()]
