"""SemanticService — stable orchestration contract.

This is the single entry point for all order-lookup logic.
Future Agentforce / Tableau Next clients call the same methods.

Pipeline:
  1. (Optional) dbt MCP → semantic context for governed meaning
  2. FuzzyService  → deterministic candidate SQL + scoring
  3. SnowflakeService → execute candidate query
  4. CortexService → parse free-text (if needed) + rerank
  5. SnowflakeService → fetch full payload for top N
  6. ExplainService → package artifacts
  7. Trace log → write to DEMO_BSC.DEMO_TRACE_LOG
"""

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from api.core.config import settings
from api.core.errors import OrderNotFoundError
from api.core.log import get_logger
from api.core.timing import Timer
from api.schemas.domain import CandidateSummary, OrderStatusPayload
from api.schemas.explain import ExplainResponse
from api.schemas.search import MatchedOrder, SearchRequest, SearchResponse, TimingsMs
from api.schemas.trace import TraceLog
from api.services.cortex_service import CortexService, RerankResult
from api.services.dbt_mcp_service import DbtMcpService
from api.services.explain_service import ExplainService
from api.services.fuzzy_service import FuzzyService, NormalizedQuery
from api.services.snowflake_service import SnowflakeService

logger = get_logger(__name__)

# SQL to fetch full payloads for a list of order_ids
_FETCH_ORDERS_SQL = """
    SELECT
        order_id, purchase_order_id, status, status_last_updated_ts,
        customer_name, facility_name, promised_delivery_date,
        carrier, tracking_number, actual_ship_ts, actual_delivery_date,
        priority_flag, requested_ship_date, total_amount_usd, currency,
        sales_region
    FROM DEMO_BSC.ORDER_SEARCH_V
    WHERE order_id IN ({placeholders})
"""

_INSERT_TRACE_SQL = """
    INSERT INTO DEMO_BSC.DEMO_TRACE_LOG (
        trace_id, created_at, mode, normalized_request_summary,
        parse_prompt_version, rerank_prompt_version,
        candidate_sql_hash, fetch_sql_hash,
        snowflake_qid_candidate, snowflake_qid_fetch,
        candidate_count, chosen_order_ids,
        sql_candidate_ms, cortex_rerank_ms, sql_fetch_top_ms, total_ms,
        error
    ) SELECT
        %(trace_id)s, %(created_at)s, %(mode)s, %(normalized_request_summary)s,
        %(parse_prompt_version)s, %(rerank_prompt_version)s,
        %(candidate_sql_hash)s, %(fetch_sql_hash)s,
        %(snowflake_qid_candidate)s, %(snowflake_qid_fetch)s,
        %(candidate_count)s, %(chosen_order_ids)s,
        %(sql_candidate_ms)s, %(cortex_rerank_ms)s, %(sql_fetch_top_ms)s, %(total_ms)s,
        %(error)s
"""


class SemanticService:
    """Stable serving contract — do not change method signatures."""

    def __init__(
        self,
        snowflake: SnowflakeService,
        cortex: CortexService,
        fuzzy: FuzzyService,
        explain: ExplainService,
        dbt_mcp: Optional[DbtMcpService] = None,
    ):
        self._sf = snowflake
        self._cortex = cortex
        self._fuzzy = fuzzy
        self._explain = explain
        self._dbt_mcp = dbt_mcp
        self._explain_store: Dict[str, ExplainResponse] = {}
        self._dbt_mcp_available = False

        if self._dbt_mcp and settings.semantic_backend == "dbt_mcp":
            self._dbt_mcp_available = self._dbt_mcp.check_availability()
            if self._dbt_mcp_available:
                logger.info("dbt_mcp_backend_active")
            else:
                logger.info("dbt_mcp_backend_unavailable, falling back to direct_sql")

    # ------------------------------------------------------------------
    # Primary entry points (stable contract)
    # ------------------------------------------------------------------

    def search_orders(self, request: SearchRequest) -> SearchResponse:
        trace_id = str(uuid.uuid4())
        timer = Timer()
        snowflake_query_ids: Dict[str, str] = {}
        error_str: Optional[str] = None
        rerank_result: RerankResult = RerankResult(
            ranked_ids=[], rationale={}, prompt_used=""
        )
        normalized: Optional[NormalizedQuery] = None
        candidate_sql = ""
        fetch_sql = ""
        candidate_count = 0
        candidates: List[CandidateSummary] = []
        semantic_objects_used: List[str] = []

        try:
            # ── Step 0a: dbt MCP semantic context (if available) ──────
            if self._dbt_mcp_available and self._dbt_mcp:
                try:
                    ctx = self._dbt_mcp.get_semantic_context_for_search()
                    semantic_objects_used = ctx.get("metrics", []) + ctx.get("dimensions", [])
                    logger.info("dbt_mcp_context", extra={"extra": {
                        "trace_id": trace_id,
                        "semantic_objects": len(semantic_objects_used),
                    }})
                except Exception as exc:
                    logger.warning(f"dbt_mcp_context_failed: {exc}")
            # ── Step 0: parse free text if needed ──────────────────────
            parse_prompt_used: Optional[str] = None
            if request.mode == "free_text" and request.free_text:
                with timer.segment("cortex_parse"):
                    intent = self._cortex.parse_user_input(request.free_text)
                    parse_prompt_used = intent.raw_response
                # Merge parsed intent into request fields (fields win if already set)
                f = request.fields
                f.order_id = f.order_id or intent.order_id
                f.purchase_order_id = f.purchase_order_id or intent.purchase_order_id
                f.customer_name = f.customer_name or intent.customer_name
                f.facility_name = f.facility_name or intent.facility_name
                if not f.date_start and intent.date_start:
                    from datetime import date
                    try:
                        f.date_start = date.fromisoformat(intent.date_start)
                    except ValueError:
                        pass
                if not f.date_end and intent.date_end:
                    from datetime import date
                    try:
                        f.date_end = date.fromisoformat(intent.date_end)
                    except ValueError:
                        pass
                f.contact_name = f.contact_name or intent.contact_name

            # ── Step 1: deterministic candidate retrieval ───────────────
            normalized = self._fuzzy.normalize_inputs(request)
            plan = self._fuzzy.build_candidate_query(normalized)
            candidate_sql = plan.sql

            with timer.segment("sql_candidate"):
                cand_result = self._sf.execute(
                    plan.sql, plan.params, label="candidate_query"
                )
                snowflake_query_ids["candidate"] = cand_result.query_id

            candidates = self._fuzzy.score_candidates(cand_result.rows)
            candidate_count = len(candidates)

            # ── Step 2: Cortex reranking ────────────────────────────────
            query_str = request.free_text or self._summarize_fields(request)

            if plan.is_exact or candidate_count == 0:
                # Skip rerank for direct ID lookups or empty results
                rerank_result = RerankResult(
                    ranked_ids=[c.order_id for c in candidates[: request.top_n]],
                    rationale={c.order_id: "Exact ID match" for c in candidates[: request.top_n]},
                    prompt_used="",
                    elapsed_ms=0.0,
                )
            else:
                rerank_pool = candidates[:20]
                with timer.segment("cortex_rerank"):
                    rerank_result = self._cortex.rerank_candidates(
                        query=query_str,
                        candidates=rerank_pool,
                        top_n=request.top_n,
                    )

            # ── Step 3: final fetch ─────────────────────────────────────
            top_ids = rerank_result.ranked_ids or [
                c.order_id for c in candidates[: request.top_n]
            ]

            matched_orders: List[MatchedOrder] = []
            if top_ids:
                fetch_sql = self._build_fetch_sql(top_ids)
                with timer.segment("sql_fetch_top"):
                    fetch_result = self._sf.execute(
                        fetch_sql,
                        {f"id_{i}": oid for i, oid in enumerate(top_ids)},
                        label="fetch_top",
                    )
                    snowflake_query_ids["fetch_top"] = fetch_result.query_id

                # Build a score lookup from candidates
                score_map = {c.order_id: c.score for c in candidates}
                rows_by_id = {row["ORDER_ID"]: row for row in fetch_result.rows}

                for order_id in top_ids:
                    row = rows_by_id.get(order_id)
                    if row is None:
                        continue
                    reasons = self._build_match_reasons(
                        row=row,
                        normalized=normalized,
                        rationale=rerank_result.rationale.get(order_id, ""),
                        is_exact=plan.is_exact,
                    )
                    matched_orders.append(
                        MatchedOrder(
                            order_id=row["ORDER_ID"],
                            purchase_order_id=row.get("PURCHASE_ORDER_ID"),
                            status=row["STATUS"],
                            status_last_updated_ts=row["STATUS_LAST_UPDATED_TS"],
                            customer_name=row["CUSTOMER_NAME"],
                            facility_name=row["FACILITY_NAME"],
                            promised_delivery_date=row.get("PROMISED_DELIVERY_DATE"),
                            carrier=row.get("CARRIER"),
                            tracking_number=row.get("TRACKING_NUMBER"),
                            actual_ship_ts=row.get("ACTUAL_SHIP_TS"),
                            actual_delivery_date=row.get("ACTUAL_DELIVERY_DATE"),
                            priority_flag=row.get("PRIORITY_FLAG"),
                            requested_ship_date=row.get("REQUESTED_SHIP_DATE"),
                            total_amount_usd=row.get("TOTAL_AMOUNT_USD"),
                            currency=row.get("CURRENCY"),
                            sales_region=row.get("SALES_REGION"),
                            match_score=round(score_map.get(order_id, 0.0), 2),
                            match_reasons=reasons,
                        )
                    )

        except Exception as exc:
            error_str = str(exc)
            logger.error(f"search_orders_error: {exc}", extra={"extra": {"trace_id": trace_id}})
            raise

        finally:
            timings = TimingsMs(
                sql_candidate_ms=timer.get("sql_candidate"),
                cortex_rerank_ms=timer.get("cortex_rerank") + getattr(rerank_result, "elapsed_ms", 0.0),
                sql_fetch_top_ms=timer.get("sql_fetch_top"),
                total_ms=timer.total_ms(),
            )
            self._write_trace(
                trace_id=trace_id,
                request=request,
                timings=timings,
                candidate_count=candidate_count,
                chosen_ids=rerank_result.ranked_ids,
                candidate_sql_hash=hashlib.sha256(candidate_sql.encode()).hexdigest()[:16],
                fetch_sql_hash=hashlib.sha256(fetch_sql.encode()).hexdigest()[:16],
                snowflake_query_ids=snowflake_query_ids,
                error=error_str,
            )

        response = SearchResponse(
            trace_id=trace_id,
            results=matched_orders,
            timings_ms=timings,
            candidate_count=candidate_count,
            candidate_sql=candidate_sql.strip(),
            fetch_sql=fetch_sql.strip(),
        )

        explain_resp = self._explain.build_explain_response(
            trace_id=trace_id,
            candidate_sql=candidate_sql,
            candidate_count=candidate_count,
            top_candidates_pre_rerank=candidates[:20],
            rerank_result=rerank_result,
            fetch_sql=fetch_sql,
            timings_ms=timings,
            prompt_versions={
                "parse": settings.parse_prompt_version,
                "rerank": settings.rerank_prompt_version,
            },
            snowflake_query_ids=snowflake_query_ids,
            parse_prompt_used=parse_prompt_used,
            normalized_request=vars(normalized) if normalized else None,
            semantic_objects_used=semantic_objects_used,
            semantic_backend=settings.semantic_backend,
        )
        self._explain_store[trace_id] = explain_resp

        return response

    def get_order_status(self, order_id: str) -> OrderStatusPayload:
        """Direct single-order lookup by exact order_id."""
        sql = """
            SELECT
                order_id, purchase_order_id, status, status_last_updated_ts,
                customer_name, facility_name, promised_delivery_date,
                carrier, tracking_number, actual_ship_ts, actual_delivery_date,
                priority_flag, requested_ship_date, total_amount_usd, currency,
                sales_region
            FROM DEMO_BSC.ORDER_SEARCH_V
            WHERE order_id = %(order_id)s
            LIMIT 1
        """
        result = self._sf.execute(sql, {"order_id": order_id}, label="get_order_status")
        if not result.rows:
            raise OrderNotFoundError(order_id)
        row = result.rows[0]
        return OrderStatusPayload(
            order_id=row["ORDER_ID"],
            purchase_order_id=row.get("PURCHASE_ORDER_ID"),
            status=row["STATUS"],
            status_last_updated_ts=row["STATUS_LAST_UPDATED_TS"],
            customer_name=row["CUSTOMER_NAME"],
            facility_name=row["FACILITY_NAME"],
            promised_delivery_date=row.get("PROMISED_DELIVERY_DATE"),
            carrier=row.get("CARRIER"),
            tracking_number=row.get("TRACKING_NUMBER"),
            actual_ship_ts=row.get("ACTUAL_SHIP_TS"),
            actual_delivery_date=row.get("ACTUAL_DELIVERY_DATE"),
            priority_flag=row.get("PRIORITY_FLAG"),
            requested_ship_date=row.get("REQUESTED_SHIP_DATE"),
            total_amount_usd=row.get("TOTAL_AMOUNT_USD"),
            currency=row.get("CURRENCY"),
            sales_region=row.get("SALES_REGION"),
        )

    def explain(self, trace_id: str) -> ExplainResponse:
        resp = self._explain_store.get(trace_id)
        if resp is None:
            raise OrderNotFoundError(f"trace:{trace_id}")
        return resp

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_fetch_sql(order_ids: List[str]) -> str:
        placeholders = ", ".join(f"%(id_{i})s" for i in range(len(order_ids)))
        return _FETCH_ORDERS_SQL.format(placeholders=placeholders)

    @staticmethod
    def _summarize_fields(request: SearchRequest) -> str:
        parts = []
        f = request.fields
        if f.order_id:
            parts.append(f"order {f.order_id}")
        if f.facility_name:
            parts.append(f"facility {f.facility_name}")
        if f.customer_name:
            parts.append(f"customer {f.customer_name}")
        if f.date_start or f.date_end:
            parts.append(f"dates {f.date_start} to {f.date_end}")
        return " ".join(parts) or "order lookup"

    @staticmethod
    def _build_match_reasons(
        row: dict,
        normalized: NormalizedQuery,
        rationale: str,
        is_exact: bool,
    ) -> List[str]:
        reasons: List[str] = []
        if is_exact:
            reasons.append("Exact ID match")
        if rationale:
            reasons.append(rationale)
        fac_norm = (row.get("FACILITY_NAME") or "").lower()
        for tok in normalized.facility_tokens:
            if tok in fac_norm:
                reasons.append(f"Facility token match: '{tok}'")
                break
        cust_norm = (row.get("CUSTOMER_NAME") or "").lower()
        for tok in normalized.customer_tokens:
            if tok in cust_norm:
                reasons.append(f"Customer name match: '{tok}'")
                break
        return reasons or ["Candidate score match"]

    def _write_trace(
        self,
        *,
        trace_id: str,
        request: SearchRequest,
        timings: TimingsMs,
        candidate_count: int,
        chosen_ids: List[str],
        candidate_sql_hash: str,
        fetch_sql_hash: str,
        snowflake_query_ids: Dict[str, str],
        error: Optional[str],
    ) -> None:
        try:
            summary = f"mode={request.mode} top_n={request.top_n}"
            if request.fields.facility_name:
                summary += f" facility={request.fields.facility_name[:20]}"

            array_literal = (
                "ARRAY_CONSTRUCT(" + ", ".join(f"'{oid}'" for oid in chosen_ids) + ")"
                if chosen_ids
                else "PARSE_JSON('[]')"
            )
            conn = self._sf._get_conn()
            cur = conn.cursor()
            cur.execute(
                _INSERT_TRACE_SQL.replace("%(chosen_order_ids)s", array_literal),
                {
                    "trace_id": trace_id,
                    "created_at": datetime.now(timezone.utc),
                    "mode": request.mode,
                    "normalized_request_summary": summary,
                    "parse_prompt_version": settings.parse_prompt_version,
                    "rerank_prompt_version": settings.rerank_prompt_version,
                    "candidate_sql_hash": candidate_sql_hash,
                    "fetch_sql_hash": fetch_sql_hash,
                    "snowflake_qid_candidate": snowflake_query_ids.get("candidate", ""),
                    "snowflake_qid_fetch": snowflake_query_ids.get("fetch_top", ""),
                    "candidate_count": candidate_count,
                    "sql_candidate_ms": timings.sql_candidate_ms,
                    "cortex_rerank_ms": timings.cortex_rerank_ms,
                    "sql_fetch_top_ms": timings.sql_fetch_top_ms,
                    "total_ms": timings.total_ms,
                    "error": error,
                },
            )
            cur.close()
        except Exception as exc:
            logger.warning(f"trace_write_failed: {exc}")
