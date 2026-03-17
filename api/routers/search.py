from fastapi import APIRouter, Depends, HTTPException
from api.schemas.search import SearchRequest, SearchResponse
from api.schemas.domain import OrderStatusPayload
from api.services.semantic_service import SemanticService
from api.core.errors import OrderNotFoundError
from api.core.log import get_logger
from api.routers.deps import get_semantic_service

logger = get_logger(__name__)
router = APIRouter()


@router.post("/search/orders", response_model=SearchResponse, tags=["search"])
def search_orders(
    request: SearchRequest,
    service: SemanticService = Depends(get_semantic_service),
) -> SearchResponse:
    """
    Main order lookup endpoint.

    Accepts structured fields or free-text mode.  Returns top N matched orders
    with status payloads, match reasons, executed SQL, and latency breakdown.

    Future clients (Agentforce, Tableau Next) should call this endpoint — the
    response schema is the stable contract.
    """
    return service.search_orders(request)


@router.get("/orders/{order_id}", response_model=OrderStatusPayload, tags=["search"])
def get_order_status(
    order_id: str,
    service: SemanticService = Depends(get_semantic_service),
) -> OrderStatusPayload:
    """Direct single-order status lookup by exact order_id."""
    try:
        return service.get_order_status(order_id)
    except OrderNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
