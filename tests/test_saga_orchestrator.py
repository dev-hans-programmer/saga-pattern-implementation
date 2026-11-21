from typing import Any

import pytest
from app.application.orchestrator import SagaOrchestrator
from app.domain.models import FailurePoint, OrderSaga, SagaStatus, StepStatus
from app.domain.repository import OrderRepository
from saga_common.contracts import MessageEnvelope


class FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, MessageEnvelope]] = []

    async def publish(
        self, exchange: str, routing_key: str, envelope: MessageEnvelope, **_: Any
    ) -> None:
        self.published.append((exchange, routing_key, envelope))


def make_saga(failure_at: FailurePoint | None = None) -> OrderSaga:
    return OrderSaga(
        customer_id="customer-1",
        sku="BOOK-001",
        quantity=2,
        amount_cents=4999,
        shipping_address="221B Baker Street",
        failure_at=failure_at,
    )


def reply(
    command: MessageEnvelope, message_type: str, payload: dict | None = None
) -> MessageEnvelope:
    return command.reply(message_type, payload or {})


@pytest.mark.asyncio
async def test_successful_saga_reaches_completed() -> None:
    repository = OrderRepository()
    bus = FakeBus()
    orchestrator = SagaOrchestrator(repository, bus)  # type: ignore[arg-type]
    saga = await orchestrator.create(make_saga(), "success-key")

    reserve = bus.published[-1][2]
    await orchestrator.handle_event(reply(reserve, "InventoryReserved"))
    charge = bus.published[-1][2]
    await orchestrator.handle_event(reply(charge, "PaymentCharged"))
    ship = bus.published[-1][2]
    await orchestrator.handle_event(reply(ship, "ShipmentScheduled"))

    saved = await repository.get(saga.order_id)
    assert saved is not None
    assert saved.status == SagaStatus.COMPLETED
    assert saved.inventory.status == StepStatus.SUCCEEDED
    assert saved.payment.status == StepStatus.SUCCEEDED
    assert saved.shipping.status == StepStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_shipping_failure_refunds_then_releases_inventory() -> None:
    repository = OrderRepository()
    bus = FakeBus()
    orchestrator = SagaOrchestrator(repository, bus)  # type: ignore[arg-type]
    saga = await orchestrator.create(make_saga(FailurePoint.SHIPPING), None)

    await orchestrator.handle_event(reply(bus.published[-1][2], "InventoryReserved"))
    await orchestrator.handle_event(reply(bus.published[-1][2], "PaymentCharged"))
    await orchestrator.handle_event(
        reply(bus.published[-1][2], "ShipmentRejected", {"reason": "carrier unavailable"})
    )
    assert bus.published[-1][2].message_type == "RefundPayment"
    await orchestrator.handle_event(reply(bus.published[-1][2], "PaymentRefunded"))
    assert bus.published[-1][2].message_type == "ReleaseInventory"
    await orchestrator.handle_event(reply(bus.published[-1][2], "InventoryReleased"))

    saved = await repository.get(saga.order_id)
    assert saved is not None
    assert saved.status == SagaStatus.COMPENSATED
    assert saved.inventory.status == StepStatus.COMPENSATED
    assert saved.payment.status == StepStatus.COMPENSATED
    assert saved.shipping.status == StepStatus.FAILED


@pytest.mark.asyncio
async def test_idempotency_key_does_not_start_two_sagas() -> None:
    repository = OrderRepository()
    bus = FakeBus()
    orchestrator = SagaOrchestrator(repository, bus)  # type: ignore[arg-type]

    first = await orchestrator.create(make_saga(), "same-request")
    second = await orchestrator.create(make_saga(), "same-request")

    assert first.order_id == second.order_id
    assert len(bus.published) == 1
