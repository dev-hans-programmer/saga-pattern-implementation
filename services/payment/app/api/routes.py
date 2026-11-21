from fastapi import APIRouter, Request

from app.domain.models import Payment

router = APIRouter()


@router.get("/payments", response_model=list[Payment])
async def list_payments(request: Request) -> list[Payment]:
    return await request.app.state.repository.list()
