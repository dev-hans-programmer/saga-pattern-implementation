from fastapi import APIRouter, Request

from app.domain.models import Shipment

router = APIRouter()


@router.get("/shipments", response_model=list[Shipment])
async def list_shipments(request: Request) -> list[Shipment]:
    return await request.app.state.repository.list()
