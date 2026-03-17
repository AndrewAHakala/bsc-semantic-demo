from pydantic import BaseModel
from typing import Optional, Dict, List
from datetime import datetime


class TraceLog(BaseModel):
    trace_id: str
    created_at: datetime
    mode: str
    normalized_request_summary: str     # redacted for PII
    parse_prompt_version: Optional[str] = None
    rerank_prompt_version: Optional[str] = None
    candidate_sql_hash: Optional[str] = None
    fetch_sql_hash: Optional[str] = None
    snowflake_query_ids: Dict[str, str] = {}
    candidate_count: int = 0
    chosen_order_ids: List[str] = []
    sql_candidate_ms: float = 0.0
    cortex_rerank_ms: float = 0.0
    sql_fetch_top_ms: float = 0.0
    total_ms: float = 0.0
    error: Optional[str] = None
