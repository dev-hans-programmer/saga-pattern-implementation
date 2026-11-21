import asyncio
import logging
from uuid import UUID

from saga_common.contracts import COMMAND_EXCHANGE, MessageEnvelope, RoutingKey
from saga_common.messaging import Inbox, RabbitBus

from app.domain.models import FailurePoint, OrderSaga, SagaStatus, StepStatus
from app.domain.repository import OrderRepository

logger = logging.getLogger(__name__)


class SagaOrchestrator:
    """The explicit state machine coordinating forward and compensating actions."""

    EVENT_KEYS = [
        RoutingKey.INVENTORY_RESERVED,
        RoutingKey.INVENTORY_REJECTED,
        RoutingKey.INVENTORY_RELEASED,
        RoutingKey.INVENTORY_RELEASE_FAILED,
        RoutingKey.PAYMENT_CHARGED,
        RoutingKey.PAYMENT_REJECTED,
        RoutingKey.PAYMENT_REFUNDED,
        RoutingKey.PAYMENT_REFUND_FAILED,
        RoutingKey.SHIPMENT_SCHEDULED,
        RoutingKey.SHIPMENT_REJECTED,
        RoutingKey.SHIPMENT_CANCELLED,
        RoutingKey.SHIPMENT_CANCEL_FAILED,
    ]

    def __init__(self, repository: OrderRepository, bus: RabbitBus) -> None:
        self._repository = repository
        self._bus = bus
        self._inbox = Inbox()
        # Serializing transitions makes the in-memory state machine deterministic.
        # A database implementation would use optimistic row versioning instead.
        self._transition_lock = asyncio.Lock()

    async def create(self, saga: OrderSaga, idempotency_key: str | None) -> OrderSaga:
        saga.inventory.status = StepStatus.IN_PROGRESS
        saved, created = await self._repository.create(saga, idempotency_key)
        if created:
            await self._send(
                saved,
                RoutingKey.RESERVE_INVENTORY,
                "ReserveInventory",
                {
                    "sku": saved.sku,
                    "quantity": saved.quantity,
                    "force_failure": saved.failure_at == FailurePoint.INVENTORY,
                },
            )
        return saved

    async def handle_event(self, event: MessageEnvelope) -> None:
        if await self._inbox.contains(event.message_id):
            logger.info("duplicate_event_ignored message_id=%s", event.message_id)
            return
        async with self._transition_lock:
            saga = await self._repository.get(event.order_id)
            if saga is None:
                logger.warning("event_for_unknown_order order_id=%s", event.order_id)
                await self._inbox.mark_processed(event.message_id)
                return
            await self._transition(saga, event)
            await self._repository.save(saga)
            await self._inbox.mark_processed(event.message_id)

    async def _transition(self, saga: OrderSaga, event: MessageEnvelope) -> None:
        key = event.message_type
        error = str(event.payload.get("reason", "downstream operation failed"))

        if key == "InventoryReserved" and saga.inventory.status == StepStatus.IN_PROGRESS:
            saga.inventory.status = StepStatus.SUCCEEDED
            saga.payment.status = StepStatus.IN_PROGRESS
            await self._send(
                saga,
                RoutingKey.CHARGE_PAYMENT,
                "ChargePayment",
                {
                    "customer_id": saga.customer_id,
                    "amount_cents": saga.amount_cents,
                    "force_failure": saga.failure_at == FailurePoint.PAYMENT,
                },
                event.message_id,
            )
        elif key == "InventoryRejected" and saga.inventory.status == StepStatus.IN_PROGRESS:
            saga.inventory.status = StepStatus.FAILED
            saga.inventory.error = error
            saga.failure_reason = error
            saga.status = SagaStatus.FAILED  # Nothing was committed, so no compensation is needed.
        elif key == "PaymentCharged" and saga.payment.status == StepStatus.IN_PROGRESS:
            saga.payment.status = StepStatus.SUCCEEDED
            saga.shipping.status = StepStatus.IN_PROGRESS
            await self._send(
                saga,
                RoutingKey.SCHEDULE_SHIPMENT,
                "ScheduleShipment",
                {
                    "shipping_address": saga.shipping_address,
                    "force_failure": saga.failure_at == FailurePoint.SHIPPING,
                },
                event.message_id,
            )
        elif key == "PaymentRejected" and saga.payment.status == StepStatus.IN_PROGRESS:
            saga.payment.status = StepStatus.FAILED
            saga.payment.error = error
            await self._begin_compensation(saga, error, event.message_id)
        elif key == "ShipmentScheduled" and saga.shipping.status == StepStatus.IN_PROGRESS:
            saga.shipping.status = StepStatus.SUCCEEDED
            if saga.failure_at == FailurePoint.FINALIZATION:
                await self._begin_compensation(
                    saga, "simulated order finalization failure", event.message_id
                )
            else:
                saga.status = SagaStatus.COMPLETED
        elif key == "ShipmentRejected" and saga.shipping.status == StepStatus.IN_PROGRESS:
            saga.shipping.status = StepStatus.FAILED
            saga.shipping.error = error
            await self._begin_compensation(saga, error, event.message_id)
        elif key == "ShipmentCancelled" and saga.shipping.status == StepStatus.COMPENSATING:
            saga.shipping.status = StepStatus.COMPENSATED
            await self._refund(saga, event.message_id)
        elif key == "PaymentRefunded" and saga.payment.status == StepStatus.COMPENSATING:
            saga.payment.status = StepStatus.COMPENSATED
            await self._release_inventory(saga, event.message_id)
        elif key == "InventoryReleased" and saga.inventory.status == StepStatus.COMPENSATING:
            saga.inventory.status = StepStatus.COMPENSATED
            saga.status = SagaStatus.COMPENSATED
        elif key in {"ShipmentCancelFailed", "PaymentRefundFailed", "InventoryReleaseFailed"}:
            saga.status = SagaStatus.COMPENSATION_FAILED
            saga.failure_reason = f"manual intervention required: {error}"
        else:
            logger.info(
                "stale_or_unexpected_event_ignored type=%s order_id=%s status=%s",
                key,
                saga.order_id,
                saga.status,
            )

    async def _begin_compensation(self, saga: OrderSaga, reason: str, causation_id: UUID) -> None:
        saga.status = SagaStatus.COMPENSATING
        saga.failure_reason = reason
        if saga.shipping.status == StepStatus.SUCCEEDED:
            saga.shipping.status = StepStatus.COMPENSATING
            await self._send(
                saga,
                RoutingKey.CANCEL_SHIPMENT,
                "CancelShipment",
                {},
                causation_id,
            )
        elif saga.payment.status == StepStatus.SUCCEEDED:
            await self._refund(saga, causation_id)
        elif saga.inventory.status == StepStatus.SUCCEEDED:
            await self._release_inventory(saga, causation_id)
        else:
            saga.status = SagaStatus.FAILED

    async def _refund(self, saga: OrderSaga, causation_id: UUID) -> None:
        saga.payment.status = StepStatus.COMPENSATING
        await self._send(saga, RoutingKey.REFUND_PAYMENT, "RefundPayment", {}, causation_id)

    async def _release_inventory(self, saga: OrderSaga, causation_id: UUID) -> None:
        saga.inventory.status = StepStatus.COMPENSATING
        await self._send(
            saga,
            RoutingKey.RELEASE_INVENTORY,
            "ReleaseInventory",
            {"sku": saga.sku, "quantity": saga.quantity},
            causation_id,
        )

    async def _send(
        self,
        saga: OrderSaga,
        routing_key: str,
        message_type: str,
        payload: dict[str, object],
        causation_id: UUID | None = None,
    ) -> None:
        command = MessageEnvelope.command(
            message_type,
            saga_id=saga.saga_id,
            order_id=saga.order_id,
            correlation_id=saga.correlation_id,
            causation_id=causation_id,
            payload=payload,
        )
        await self._bus.publish(COMMAND_EXCHANGE, routing_key, command)
