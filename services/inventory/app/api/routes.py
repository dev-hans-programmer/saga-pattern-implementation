from fastapi import APIRouter, Request

from app.domain.models import Reservation, StockItem

router = APIRouter()


@router.get("/inventory/{sku}", response_model=StockItem)
async def get_stock(sku: str, request: Request) -> StockItem:
    return await request.app.state.repository.stock(sku)


@router.get("/reservations", response_model=list[Reservation])
async def list_reservations(request: Request) -> list[Reservation]:
    return await request.app.state.repository.reservations()
