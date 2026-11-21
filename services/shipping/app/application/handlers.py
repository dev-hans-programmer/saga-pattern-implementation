import logging

from saga_common.contracts import EVENT_EXCHANGE, MessageEnvelope, RoutingKey
from saga_common.messaging import Inbox, RabbitBus

from app.domain.repository import ShippingRepository

logger = logging.getLogger(__name__)


class ShippingCommandHandler:
    COMMAND_KEYS = [RoutingKey.SCHEDULE_SHIPMENT, RoutingKey.CANCEL_SHIPMENT]

    def __init__(self, repository: ShippingRepository, bus: RabbitBus) -> None:
        self._repository = repository
        self._bus = bus
        self._inbox = Inbox()

    async def __call__(self, command: MessageEnvelope) -> None:
        if await self._inbox.contains(command.message_id):
            logger.info("duplicate_command_ignored message_id=%s", command.message_id)
            return
        if command.message_type == "ScheduleShipment":
            await self._schedule(command)
        elif command.message_type == "CancelShipment":
            await self._cancel(command)
        else:
            raise ValueError(f"unsupported command: {command.message_type}")
        await self._inbox.mark_processed(command.message_id)

    async def _schedule(self, command: MessageEnvelope) -> None:
        try:
            if command.payload.get("force_failure"):
                raise ValueError("simulated shipment scheduling failure")
            shipment = await self._repository.schedule(
                command.order_id, str(command.payload["shipping_address"])
            )
            event = command.reply("ShipmentScheduled", {"shipment_id": str(shipment.shipment_id)})
            key = RoutingKey.SHIPMENT_SCHEDULED
        except (ValueError, KeyError) as exc:
            event = command.reply("ShipmentRejected", {"reason": str(exc)})
            key = RoutingKey.SHIPMENT_REJECTED
        await self._bus.publish(EVENT_EXCHANGE, key, event)

    async def _cancel(self, command: MessageEnvelope) -> None:
        try:
            shipment = await self._repository.cancel(command.order_id)
            event = command.reply("ShipmentCancelled", {"shipment_id": str(shipment.shipment_id)})
            key = RoutingKey.SHIPMENT_CANCELLED
        except ValueError as exc:
            event = command.reply("ShipmentCancelFailed", {"reason": str(exc)})
            key = RoutingKey.SHIPMENT_CANCEL_FAILED
        await self._bus.publish(EVENT_EXCHANGE, key, event)
