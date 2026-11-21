from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class SagaStatus(StrEnum):
    RUNNING = "running"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    COMPENSATED = "compensated"
    FAILED = "failed"
    COMPENSATION_FAILED = "compensation_failed"


class StepStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


class FailurePoint(StrEnum):
    INVENTORY = "inventory"
    PAYMENT = "payment"
    SHIPPING = "shipping"
    FINALIZATION = "finalization"


class SagaStep(BaseModel):
    name: str
    status: StepStatus = StepStatus.PENDING
    error: str | None = None


class OrderSaga(BaseModel):
    order_id: UUID = Field(default_factory=uuid4)
    saga_id: UUID = Field(default_factory=uuid4)
    correlation_id: UUID = Field(default_factory=uuid4)
    customer_id: str
    sku: str
    quantity: int
    amount_cents: int
    shipping_address: str
    failure_at: FailurePoint | None = None
    status: SagaStatus = SagaStatus.RUNNING
    failure_reason: str | None = None
    inventory: SagaStep = Field(default_factory=lambda: SagaStep(name="reserve_inventory"))
    payment: SagaStep = Field(default_factory=lambda: SagaStep(name="charge_payment"))
    shipping: SagaStep = Field(default_factory=lambda: SagaStep(name="schedule_shipping"))
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def touch(self) -> None:
        self.updated_at = datetime.now(UTC)
