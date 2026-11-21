import asyncio
from uuid import UUID

from .models import Reservation, ReservationStatus, StockItem


class InventoryRepository:
    def __init__(self, initial_stock: int = 100) -> None:
        self._default_stock = initial_stock
        self._stock: dict[str, int] = {}
        self._reservations: dict[UUID, Reservation] = {}
        self._lock = asyncio.Lock()

    async def reserve(self, order_id: UUID, sku: str, quantity: int) -> Reservation:
        async with self._lock:
            existing = self._reservations.get(order_id)
            if existing:
                # Returning the original result makes redelivery harmless, even
                # if the first acknowledgement was lost after committing.
                return existing.model_copy(deep=True)
            available = self._stock.setdefault(sku, self._default_stock)
            if available < quantity:
                raise ValueError(f"insufficient stock for {sku}: {available} available")
            self._stock[sku] -= quantity
            reservation = Reservation(order_id=order_id, sku=sku, quantity=quantity)
            self._reservations[order_id] = reservation
            return reservation.model_copy(deep=True)

    async def release(self, order_id: UUID) -> Reservation:
        async with self._lock:
            reservation = self._reservations.get(order_id)
            if reservation is None:
                raise ValueError("reservation does not exist")
            if reservation.status == ReservationStatus.ACTIVE:
                self._stock[reservation.sku] += reservation.quantity
                reservation.status = ReservationStatus.RELEASED
            return reservation.model_copy(deep=True)

    async def stock(self, sku: str) -> StockItem:
        async with self._lock:
            return StockItem(sku=sku, available=self._stock.setdefault(sku, self._default_stock))

    async def reservations(self) -> list[Reservation]:
        async with self._lock:
            return [item.model_copy(deep=True) for item in self._reservations.values()]
