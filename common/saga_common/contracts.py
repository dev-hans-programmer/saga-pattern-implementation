from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

COMMAND_EXCHANGE = "saga.commands"
EVENT_EXCHANGE = "saga.events"
DEAD_LETTER_EXCHANGE = "saga.dead-letters"


class MessageEnvelope(BaseModel):
    """Stable metadata shared by every command and event.

    The IDs make distributed logs traceable and allow consumers to reject
    duplicate deliveries, which are normal with at-least-once messaging.
    """

    message_id: UUID = Field(default_factory=uuid4)
    message_type: str
    saga_id: UUID
    order_id: UUID
    correlation_id: UUID
    causation_id: UUID | None = None
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    schema_version: int = 1
    payload: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def command(
        cls,
        message_type: str,
        *,
        saga_id: UUID,
        order_id: UUID,
        correlation_id: UUID,
        causation_id: UUID | None,
        payload: dict[str, Any],
    ) -> "MessageEnvelope":
        return cls(
            message_type=message_type,
            saga_id=saga_id,
            order_id=order_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload=payload,
        )

    def reply(self, message_type: str, payload: dict[str, Any]) -> "MessageEnvelope":
        return MessageEnvelope(
            message_type=message_type,
            saga_id=self.saga_id,
            order_id=self.order_id,
            correlation_id=self.correlation_id,
            causation_id=self.message_id,
            payload=payload,
        )


class RoutingKey:
    RESERVE_INVENTORY = "inventory.reserve"
    RELEASE_INVENTORY = "inventory.release"
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_REJECTED = "inventory.rejected"
    INVENTORY_RELEASED = "inventory.released"
    INVENTORY_RELEASE_FAILED = "inventory.release_failed"

    CHARGE_PAYMENT = "payment.charge"
    REFUND_PAYMENT = "payment.refund"
    PAYMENT_CHARGED = "payment.charged"
    PAYMENT_REJECTED = "payment.rejected"
    PAYMENT_REFUNDED = "payment.refunded"
    PAYMENT_REFUND_FAILED = "payment.refund_failed"

    SCHEDULE_SHIPMENT = "shipping.schedule"
    CANCEL_SHIPMENT = "shipping.cancel"
    SHIPMENT_SCHEDULED = "shipping.scheduled"
    SHIPMENT_REJECTED = "shipping.rejected"
    SHIPMENT_CANCELLED = "shipping.cancelled"
    SHIPMENT_CANCEL_FAILED = "shipping.cancel_failed"
