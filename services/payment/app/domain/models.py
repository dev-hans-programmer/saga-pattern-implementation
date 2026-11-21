from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class PaymentStatus(StrEnum):
    CHARGED = "charged"
    REFUNDED = "refunded"


class Payment(BaseModel):
    payment_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    customer_id: str
    amount_cents: int
    status: PaymentStatus = PaymentStatus.CHARGED
