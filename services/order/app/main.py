from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from saga_common.config import get_settings
from saga_common.contracts import EVENT_EXCHANGE
from saga_common.logging import configure_logging
from saga_common.messaging import RabbitBus

from app.api.routes import router
from app.application.orchestrator import SagaOrchestrator
from app.domain.repository import OrderRepository

settings = get_settings("order-service")
configure_logging(settings.service_name, settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    repository = OrderRepository()
    bus = RabbitBus(settings.rabbitmq_url, settings.service_name)
    await bus.connect()
    orchestrator = SagaOrchestrator(repository, bus)
    await bus.consume(
        exchange_name=EVENT_EXCHANGE,
        queue_name="order-service.events",
        routing_keys=orchestrator.EVENT_KEYS,
        handler=orchestrator.handle_event,
    )
    app.state.repository = repository
    app.state.bus = bus
    app.state.orchestrator = orchestrator
    yield
    await bus.close()


app = FastAPI(
    title="Order Service / Saga Orchestrator",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(router)


@app.get("/health/live", tags=["health"])
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready", tags=["health"])
async def readiness(request: Request) -> dict[str, str]:
    return {"status": "ready" if request.app.state.bus.is_connected else "not_ready"}
