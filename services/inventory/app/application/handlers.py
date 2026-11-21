import logging

from saga_common.contracts import EVENT_EXCHANGE, MessageEnvelope, RoutingKey
from saga_common.messaging import Inbox, RabbitBus

from app.domain.repository import InventoryRepository

logger = logging.getLogger(__name__)


class InventoryCommandHandler:
    COMMAND_KEYS = [RoutingKey.RESERVE_INVENTORY, RoutingKey.RELEASE_INVENTORY]

    def __init__(self, repository: InventoryRepository, bus: RabbitBus) -> None:
        self._repository = repository
        self._bus = bus
        self._inbox = Inbox()

    async def __call__(self, command: MessageEnvelope) -> None:
        if await self._inbox.contains(command.message_id):
            logger.info("duplicate_command_ignored message_id=%s", command.message_id)
            return
        if command.message_type == "ReserveInventory":
            await self._reserve(command)
        elif command.message_type == "ReleaseInventory":
            await self._release(command)
        else:
            raise ValueError(f"unsupported command: {command.message_type}")
        await self._inbox.mark_processed(command.message_id)

    async def _reserve(self, command: MessageEnvelope) -> None:
        try:
            if command.payload.get("force_failure"):
                raise ValueError("simulated inventory reservation failure")
            reservation = await self._repository.reserve(
                command.order_id,
                str(command.payload["sku"]),
                int(command.payload["quantity"]),
            )
            event = command.reply(
                "InventoryReserved",
                {"sku": reservation.sku, "quantity": reservation.quantity},
            )
            key = RoutingKey.INVENTORY_RESERVED
        except (ValueError, KeyError) as exc:
            event = command.reply("InventoryRejected", {"reason": str(exc)})
            key = RoutingKey.INVENTORY_REJECTED
        await self._bus.publish(EVENT_EXCHANGE, key, event)

    async def _release(self, command: MessageEnvelope) -> None:
        try:
            reservation = await self._repository.release(command.order_id)
            event = command.reply(
                "InventoryReleased",
                {"sku": reservation.sku, "quantity": reservation.quantity},
            )
            key = RoutingKey.INVENTORY_RELEASED
        except ValueError as exc:
            event = command.reply("InventoryReleaseFailed", {"reason": str(exc)})
            key = RoutingKey.INVENTORY_RELEASE_FAILED
        await self._bus.publish(EVENT_EXCHANGE, key, event)
