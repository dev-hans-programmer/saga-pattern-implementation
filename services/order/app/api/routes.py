from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.domain.models import FailurePoint, OrderSaga

router = APIRouter()


class CreateOrderRequest(BaseModel):
    customer_id: str = Field(min_length=1, max_length=100)
    sku: str = Field(min_length=1, max_length=100)
    quantity: int = Field(gt=0, le=100)
    amount_cents: int = Field(gt=0)
    shipping_address: str = Field(min_length=3, max_length=500)
    failure_at: FailurePoint | None = None


@router.post("/orders", response_model=OrderSaga, status_code=status.HTTP_202_ACCEPTED)
async def create_order(
    body: CreateOrderRequest,
    request: Request,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> OrderSaga:
    saga = OrderSaga(**body.model_dump())
    return await request.app.state.orchestrator.create(saga, idempotency_key)


@router.get("/orders/{order_id}", response_model=OrderSaga)
async def get_order(order_id: UUID, request: Request) -> OrderSaga:
    order = await request.app.state.repository.get(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    return order


@router.get("/orders", response_model=list[OrderSaga])
async def list_orders(request: Request) -> list[OrderSaga]:
    return await request.app.state.repository.list()
