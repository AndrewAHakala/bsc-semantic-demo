"""Cortex integration — LLM usage is strictly bounded to:
  1. parse_user_input  → extract structured fields from free text
  2. rerank_candidates → sort candidate list by relevance

Guardrails:
  - Cortex cannot invent order IDs; reranker may only pick from provided candidates.
  - All unknown IDs returned by the model are silently filtered out.
  - Results are cached by a normalized cache key (TTL configurable).
"""

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from cachetools import TTLCache

from api.core.config import settings
from api.core.errors import CortexError
from api.core.log import get_logger
from api.schemas.domain import CandidateSummary
from api.schemas.search import SearchFields
from api.services.snowflake_service import SnowflakeService

logger = get_logger(__name__)


@dataclass
class ParsedIntent:
    order_id: Optional[str] = None
    purchase_order_id: Optional[str] = None
    customer_name: Optional[str] = None
    facility_name: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    contact_name: Optional[str] = None
    raw_response: str = ""


@dataclass
class RerankResult:
    ranked_ids: List[str]
    rationale: Dict[str, str]   # order_id -> reason
    prompt_used: str
    elapsed_ms: float = 0.0


_PARSE_PROMPT_TEMPLATE = """\
You are an order lookup assistant for a medical device company.
Extract structured fields from the user's query. Return ONLY a valid JSON object.
Do not explain. Do not add markdown. Return null for unknown fields.

Fields to extract:
  order_id: string or null (e.g. "SO-2026-001234" or partial like "01234")
  purchase_order_id: string or null
  customer_name: string or null
  facility_name: string or null (hospital / clinic / facility name)
  date_start: ISO date string or null (start of date window)
  date_end: ISO date string or null (end of date window)
  contact_name: string or null

Today's date: {today}

User query: {query}

Return JSON only."""


_RERANK_PROMPT_TEMPLATE = """\
You are an order matching assistant for a medical device fulfillment company.
A customer support rep is on a live call and needs the most relevant orders.

User query: {query}

Candidate orders (JSON):
{candidates_json}

Instructions:
1. Rank the candidates by relevance to the user query (most relevant first).
2. You MUST only use order_ids from the candidates list above.
3. Return at most {top_n} order_ids.
4. For each chosen order, give a SHORT reason (1 sentence) why it matches.

Return ONLY a valid JSON object in this exact format:
{{
  "ranked_ids": ["order_id_1", "order_id_2"],
  "rationale": {{
    "order_id_1": "Matches facility name and date window",
    "order_id_2": "Status recently updated, facility name partial match"
  }}
}}"""


def _cache_key(*parts: Any) -> str:
    raw = json.dumps(parts, default=str, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


class CortexService:

    def __init__(self, snowflake: SnowflakeService):
        self._sf = snowflake
        self._rerank_cache: TTLCache = TTLCache(
            maxsize=512, ttl=settings.rerank_cache_ttl_s
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse_user_input(self, text: str) -> ParsedIntent:
        """Call Cortex to extract structured fields from free-form text."""
        from datetime import date
        today = date.today().isoformat()
        prompt = _PARSE_PROMPT_TEMPLATE.format(today=today, query=text)

        raw = self._complete(prompt, label="parse_user_input")
        intent = self._safe_parse_intent(raw)
        intent.raw_response = raw
        return intent

    def rerank_candidates(
        self,
        query: str,
        candidates: List[CandidateSummary],
        top_n: int,
    ) -> RerankResult:
        """Rerank candidates using Cortex. Results are cached by (query, candidate_ids)."""
        candidate_ids = [c.order_id for c in candidates]
        cache_key = _cache_key(query, candidate_ids, top_n)

        if cache_key in self._rerank_cache:
            logger.info("rerank_cache_hit", extra={"extra": {"key": cache_key[:12]}})
            return self._rerank_cache[cache_key]

        candidates_json = json.dumps(
            [
                {
                    "order_id": c.order_id,
                    "purchase_order_id": c.purchase_order_id,
                    "status": c.status,
                    "status_last_updated_ts": str(c.status_last_updated_ts),
                    "customer_name": c.customer_name,
                    "facility_name": c.facility_name,
                    "promised_delivery_date": str(c.promised_delivery_date) if c.promised_delivery_date else None,
                    "tracking_number": c.tracking_number,
                    "candidate_score": round(c.score, 2),
                }
                for c in candidates
            ],
            indent=2,
        )

        prompt = _RERANK_PROMPT_TEMPLATE.format(
            query=query,
            candidates_json=candidates_json,
            top_n=top_n,
        )

        t0 = time.perf_counter()
        raw = self._complete(prompt, label="rerank_candidates")
        elapsed_ms = (time.perf_counter() - t0) * 1000

        result = self._safe_parse_rerank(raw, valid_ids=set(candidate_ids), top_n=top_n)
        result.prompt_used = prompt
        result.elapsed_ms = round(elapsed_ms, 1)

        self._rerank_cache[cache_key] = result
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _complete(self, prompt: str, *, label: str = "cortex") -> str:
        """Call SNOWFLAKE.CORTEX.COMPLETE and return the text response."""
        sql = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                %(model)s,
                ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT('role', 'user', 'content', %(prompt)s)
                )
            ) AS response
        """
        try:
            result = self._sf.execute(
                sql,
                {"model": settings.cortex_model, "prompt": prompt},
                label=label,
            )
            if not result.rows:
                raise CortexError(f"Empty response from Cortex [{label}]")
            raw = result.rows[0].get("RESPONSE", "") or ""
            # Cortex returns a JSON string with a choices array
            return self._extract_content(raw)
        except Exception as exc:
            raise CortexError(str(exc)) from exc

    @staticmethod
    def _extract_content(raw: str) -> str:
        """Pull text from Cortex COMPLETE JSON envelope if present."""
        try:
            parsed = json.loads(raw)
            # Standard Cortex envelope: {"choices": [{"messages": "..."}]}
            if isinstance(parsed, dict) and "choices" in parsed:
                return parsed["choices"][0].get("messages", raw)
        except (json.JSONDecodeError, KeyError, IndexError):
            pass
        return raw

    @staticmethod
    def _extract_json_block(text: str) -> str:
        """Strip markdown fences and return first JSON block found."""
        # Remove ```json ... ``` fences
        text = re.sub(r"```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = text.replace("```", "").strip()
        # Find first { ... } block
        match = re.search(r"\{.*\}", text, re.DOTALL)
        return match.group(0) if match else text

    def _safe_parse_intent(self, raw: str) -> ParsedIntent:
        try:
            data = json.loads(self._extract_json_block(raw))
            return ParsedIntent(
                order_id=data.get("order_id"),
                purchase_order_id=data.get("purchase_order_id"),
                customer_name=data.get("customer_name"),
                facility_name=data.get("facility_name"),
                date_start=data.get("date_start"),
                date_end=data.get("date_end"),
                contact_name=data.get("contact_name"),
            )
        except Exception as exc:
            logger.warning(f"parse_intent_failed: {exc} | raw={raw[:200]}")
            return ParsedIntent()

    def _safe_parse_rerank(
        self, raw: str, valid_ids: set, top_n: int
    ) -> RerankResult:
        try:
            data = json.loads(self._extract_json_block(raw))
            ranked_ids = [
                oid for oid in data.get("ranked_ids", []) if oid in valid_ids
            ][:top_n]
            rationale = {
                oid: str(reason)
                for oid, reason in data.get("rationale", {}).items()
                if oid in valid_ids
            }
            return RerankResult(ranked_ids=ranked_ids, rationale=rationale, prompt_used="")
        except Exception as exc:
            logger.warning(f"rerank_parse_failed: {exc} | raw={raw[:200]}")
            # Fallback: return ids in original order
            return RerankResult(
                ranked_ids=list(valid_ids)[:top_n],
                rationale={},
                prompt_used="",
            )
