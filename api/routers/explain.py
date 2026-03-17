from fastapi import APIRouter, Depends, HTTPException
from api.schemas.explain import ExplainResponse
from api.services.semantic_service import SemanticService
from api.core.errors import OrderNotFoundError
from api.routers.deps import get_semantic_service

router = APIRouter()


@router.get("/explain/{trace_id}", response_model=ExplainResponse, tags=["explain"])
def explain(
    trace_id: str,
    service: SemanticService = Depends(get_semantic_service),
) -> ExplainResponse:
    """
    Return full explainability artifact for a completed search request.

    Includes: candidate SQL, rerank rationale, final fetch SQL, timings,
    prompt versions, and Snowflake query IDs.
    """
    try:
        return service.explain(trace_id)
    except OrderNotFoundError:
        raise HTTPException(status_code=404, detail=f"No explain data for trace_id={trace_id}")
