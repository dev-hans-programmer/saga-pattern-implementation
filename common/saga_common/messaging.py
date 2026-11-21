from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, IncomingMessage, Message

from .contracts import DEAD_LETTER_EXCHANGE, MessageEnvelope

MessageHandler = Callable[[MessageEnvelope], Awaitable[None]]
logger = logging.getLogger(__name__)


class RabbitBus:
    """Small aio-pika adapter with publisher confirms and bounded retries."""

    def __init__(self, url: str, service_name: str) -> None:
        self._url = url
        self._service_name = service_name
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._exchanges: dict[str, aio_pika.abc.AbstractRobustExchange] = {}
        self._consumer_tags: list[tuple[aio_pika.abc.AbstractRobustQueue, str]] = []

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and not self._connection.is_closed

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=20)
        logger.info("connected_to_rabbitmq")

    async def _exchange(self, name: str) -> aio_pika.abc.AbstractRobustExchange:
        if self._channel is None:
            raise RuntimeError("RabbitBus.connect() must be called first")
        if name not in self._exchanges:
            self._exchanges[name] = await self._channel.declare_exchange(
                name, ExchangeType.TOPIC, durable=True
            )
        return self._exchanges[name]

    async def publish(
        self,
        exchange_name: str,
        routing_key: str,
        envelope: MessageEnvelope,
        *,
        retry_count: int = 0,
    ) -> None:
        exchange = await self._exchange(exchange_name)
        await exchange.publish(
            Message(
                envelope.model_dump_json().encode(),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=str(envelope.message_id),
                correlation_id=str(envelope.correlation_id),
                headers={"x-retry-count": retry_count},
            ),
            routing_key=routing_key,
        )
        logger.info(
            "message_published type=%s routing_key=%s saga_id=%s",
            envelope.message_type,
            routing_key,
            envelope.saga_id,
        )

    async def consume(
        self,
        *,
        exchange_name: str,
        queue_name: str,
        routing_keys: list[str],
        handler: MessageHandler,
        max_retries: int = 3,
    ) -> None:
        if self._channel is None:
            raise RuntimeError("RabbitBus.connect() must be called first")
        exchange = await self._exchange(exchange_name)
        dead_exchange = await self._exchange(DEAD_LETTER_EXCHANGE)
        dead_queue = await self._channel.declare_queue(f"{queue_name}.dlq", durable=True)
        await dead_queue.bind(dead_exchange, routing_key=queue_name)
        queue = await self._channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
                "x-dead-letter-routing-key": queue_name,
            },
        )
        for routing_key in routing_keys:
            await queue.bind(exchange, routing_key=routing_key)

        async def dispatch(message: IncomingMessage) -> None:
            async with message.process(requeue=False):
                envelope = MessageEnvelope.model_validate_json(message.body)
                try:
                    await handler(envelope)
                except Exception:
                    retry_count = int((message.headers or {}).get("x-retry-count", 0))
                    if retry_count >= max_retries:
                        logger.exception(
                            "message_dead_lettered type=%s saga_id=%s",
                            envelope.message_type,
                            envelope.saga_id,
                        )
                        raise
                    # A production system usually uses delayed retry queues or a
                    # broker plugin. A short bounded delay keeps this demo portable.
                    await asyncio.sleep(0.25 * (2**retry_count))
                    await self.publish(
                        exchange_name,
                        message.routing_key,
                        envelope,
                        retry_count=retry_count + 1,
                    )
                    logger.warning(
                        "message_retried type=%s attempt=%s",
                        envelope.message_type,
                        retry_count + 1,
                    )

        tag = await queue.consume(dispatch)
        self._consumer_tags.append((queue, tag))
        logger.info("consumer_started queue=%s keys=%s", queue_name, routing_keys)

    async def close(self) -> None:
        for queue, tag in self._consumer_tags:
            await queue.cancel(tag)
        self._consumer_tags.clear()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        logger.info("rabbitmq_connection_closed")


class Inbox:
    """In-memory inbox used to make consumers idempotent within a process lifetime."""

    def __init__(self) -> None:
        self._processed: set[str] = set()
        self._lock = asyncio.Lock()

    async def contains(self, message_id: object) -> bool:
        async with self._lock:
            return str(message_id) in self._processed

    async def mark_processed(self, message_id: object) -> None:
        async with self._lock:
            self._processed.add(str(message_id))
