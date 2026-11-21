import asyncio
from uuid import UUID

from .models import OrderSaga


class OrderRepository:
    """Process-local repository; the interface is the seam for PostgreSQL later."""

    def __init__(self) -> None:
        self._orders: dict[UUID, OrderSaga] = {}
        self._idempotency_keys: dict[str, UUID] = {}
        self._lock = asyncio.Lock()

    async def create(self, saga: OrderSaga, idempotency_key: str | None) -> tuple[OrderSaga, bool]:
        async with self._lock:
            if idempotency_key and idempotency_key in self._idempotency_keys:
                existing_id = self._idempotency_keys[idempotency_key]
                return self._orders[existing_id].model_copy(deep=True), False
            self._orders[saga.order_id] = saga.model_copy(deep=True)
            if idempotency_key:
                self._idempotency_keys[idempotency_key] = saga.order_id
            return saga.model_copy(deep=True), True

    async def get(self, order_id: UUID) -> OrderSaga | None:
        async with self._lock:
            order = self._orders.get(order_id)
            return order.model_copy(deep=True) if order else None

    async def save(self, saga: OrderSaga) -> None:
        async with self._lock:
            saga.touch()
            self._orders[saga.order_id] = saga.model_copy(deep=True)

    async def list(self) -> list[OrderSaga]:
        async with self._lock:
            return [item.model_copy(deep=True) for item in self._orders.values()]
