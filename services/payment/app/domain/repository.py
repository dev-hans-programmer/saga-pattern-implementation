import asyncio
from uuid import UUID

from .models import Payment, PaymentStatus


class PaymentRepository:
    def __init__(self) -> None:
        self._payments: dict[UUID, Payment] = {}
        self._lock = asyncio.Lock()

    async def charge(self, order_id: UUID, customer_id: str, amount_cents: int) -> Payment:
        async with self._lock:
            existing = self._payments.get(order_id)
            if existing:
                # A refunded payment is never charged again by a late duplicate.
                return existing.model_copy(deep=True)
            payment = Payment(
                order_id=order_id,
                customer_id=customer_id,
                amount_cents=amount_cents,
            )
            self._payments[order_id] = payment
            return payment.model_copy(deep=True)

    async def refund(self, order_id: UUID) -> Payment:
        async with self._lock:
            payment = self._payments.get(order_id)
            if payment is None:
                raise ValueError("payment does not exist")
            payment.status = PaymentStatus.REFUNDED
            return payment.model_copy(deep=True)

    async def list(self) -> list[Payment]:
        async with self._lock:
            return [payment.model_copy(deep=True) for payment in self._payments.values()]
