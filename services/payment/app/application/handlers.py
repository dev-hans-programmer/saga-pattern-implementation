import logging

from saga_common.contracts import EVENT_EXCHANGE, MessageEnvelope, RoutingKey
from saga_common.messaging import Inbox, RabbitBus

from app.domain.repository import PaymentRepository

logger = logging.getLogger(__name__)


class PaymentCommandHandler:
    COMMAND_KEYS = [RoutingKey.CHARGE_PAYMENT, RoutingKey.REFUND_PAYMENT]

    def __init__(self, repository: PaymentRepository, bus: RabbitBus) -> None:
        self._repository = repository
        self._bus = bus
        self._inbox = Inbox()

    async def __call__(self, command: MessageEnvelope) -> None:
        if await self._inbox.contains(command.message_id):
            logger.info("duplicate_command_ignored message_id=%s", command.message_id)
            return
        if command.message_type == "ChargePayment":
            await self._charge(command)
        elif command.message_type == "RefundPayment":
            await self._refund(command)
        else:
            raise ValueError(f"unsupported command: {command.message_type}")
        await self._inbox.mark_processed(command.message_id)

    async def _charge(self, command: MessageEnvelope) -> None:
        try:
            if command.payload.get("force_failure"):
                raise ValueError("simulated payment authorization failure")
            payment = await self._repository.charge(
                command.order_id,
                str(command.payload["customer_id"]),
                int(command.payload["amount_cents"]),
            )
            event = command.reply(
                "PaymentCharged",
                {"payment_id": str(payment.payment_id), "amount_cents": payment.amount_cents},
            )
            key = RoutingKey.PAYMENT_CHARGED
        except (ValueError, KeyError) as exc:
            event = command.reply("PaymentRejected", {"reason": str(exc)})
            key = RoutingKey.PAYMENT_REJECTED
        await self._bus.publish(EVENT_EXCHANGE, key, event)

    async def _refund(self, command: MessageEnvelope) -> None:
        try:
            payment = await self._repository.refund(command.order_id)
            event = command.reply(
                "PaymentRefunded",
                {"payment_id": str(payment.payment_id), "amount_cents": payment.amount_cents},
            )
            key = RoutingKey.PAYMENT_REFUNDED
        except ValueError as exc:
            event = command.reply("PaymentRefundFailed", {"reason": str(exc)})
            key = RoutingKey.PAYMENT_REFUND_FAILED
        await self._bus.publish(EVENT_EXCHANGE, key, event)
