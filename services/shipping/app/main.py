from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from saga_common.config import get_settings
from saga_common.contracts import COMMAND_EXCHANGE
from saga_common.logging import configure_logging
from saga_common.messaging import RabbitBus

from app.api.routes import router
from app.application.handlers import ShippingCommandHandler
from app.domain.repository import ShippingRepository

settings = get_settings("shipping-service")
configure_logging(settings.service_name, settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    repository = ShippingRepository()
    bus = RabbitBus(settings.rabbitmq_url, settings.service_name)
    await bus.connect()
    handler = ShippingCommandHandler(repository, bus)
    await bus.consume(
        exchange_name=COMMAND_EXCHANGE,
        queue_name="shipping-service.commands",
        routing_keys=handler.COMMAND_KEYS,
        handler=handler,
    )
    app.state.repository = repository
    app.state.bus = bus
    yield
    await bus.close()


app = FastAPI(title="Shipping Service", version="1.0.0", lifespan=lifespan)
app.include_router(router)


@app.get("/health/live", tags=["health"])
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready", tags=["health"])
async def readiness(request: Request) -> dict[str, str]:
    return {"status": "ready" if request.app.state.bus.is_connected else "not_ready"}
