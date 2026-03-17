from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import date
from .domain import OrderStatusPayload


class SearchFields(BaseModel):
    order_id: Optional[str] = None
    purchase_order_id: Optional[str] = None
    customer_name: Optional[str] = None
    facility_name: Optional[str] = None
    date_start: Optional[date] = None
    date_end: Optional[date] = None
    contact_name: Optional[str] = None


class SearchRequest(BaseModel):
    mode: Literal["structured", "free_text"] = "structured"
    free_text: Optional[str] = None
    fields: SearchFields = Field(default_factory=SearchFields)
    top_n: int = Field(default=5, ge=1, le=20)


class MatchedOrder(OrderStatusPayload):
    match_score: float
    match_reasons: List[str]


class TimingsMs(BaseModel):
    sql_candidate_ms: float
    cortex_rerank_ms: float
    sql_fetch_top_ms: float
    total_ms: float


class SearchResponse(BaseModel):
    trace_id: str
    results: List[MatchedOrder]
    timings_ms: TimingsMs
    candidate_count: int
    candidate_sql: Optional[str] = None
    fetch_sql: Optional[str] = None
