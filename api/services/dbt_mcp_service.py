"""dbt Cloud Semantic Layer integration via remote MCP / REST API.

When semantic_backend = 'dbt_mcp', the SemanticService uses this adapter to:
  1. Enumerate available semantic objects (metrics, dimensions, entities)
  2. Compile/generate SQL from semantic definitions via dbt Cloud
  3. Log the semantic objects used for explainability

The adapter calls the dbt Cloud remote MCP endpoint, which exposes tools
like list_metrics, list_dimensions, list_entities, query_metrics, and
get_metrics_compiled_sql.

Falls back gracefully to direct SQL when dbt Cloud is unreachable.
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx

from api.core.config import settings
from api.core.log import get_logger

logger = get_logger(__name__)

_MCP_TIMEOUT = 15.0


@dataclass
class SemanticObject:
    name: str
    object_type: str  # metric, dimension, entity, semantic_model
    description: str = ""
    expr: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SemanticQueryResult:
    sql: str
    semantic_objects_used: List[str]
    compile_ms: float = 0.0
    meta: Dict[str, Any] = field(default_factory=dict)


class DbtMcpService:
    """Adapter for dbt Cloud Semantic Layer via the remote MCP endpoint.

    Calls dbt Cloud's remote MCP HTTP API to enumerate and query
    semantic objects. When dbt Cloud is unreachable, all methods return
    empty results and log warnings — the application continues via
    direct SQL fallback.
    """

    def __init__(self):
        self._host = settings.dbt_cloud_host.rstrip("/")
        self._token = settings.dbt_cloud_token
        self._env_id = settings.dbt_cloud_environment_id
        self._available = False
        self._semantic_objects_cache: Optional[List[SemanticObject]] = None
        self._cache_ts: float = 0.0

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Token {self._token}",
            "x-dbt-prod-environment-id": self._env_id,
            "Content-Type": "application/json",
        }

    @property
    def is_available(self) -> bool:
        return self._available

    def check_availability(self) -> bool:
        """Probe dbt Cloud to see if the Semantic Layer is reachable."""
        if not self._host or not self._token or not self._env_id:
            logger.info("dbt_cloud_not_configured: missing host, token, or environment_id")
            self._available = False
            return False

        try:
            resp = httpx.get(
                f"{self._host}/api/ai/v1/mcp/",
                headers=self._headers(),
                timeout=_MCP_TIMEOUT,
            )
            # 406 means the server is reachable and auth is valid (it wants SSE)
            self._available = resp.status_code in (200, 406)
            if self._available:
                logger.info("dbt_cloud_connected", extra={"extra": {"host": self._host}})
            else:
                logger.warning(f"dbt_cloud_unexpected_status: {resp.status_code}")
        except Exception as exc:
            logger.warning(f"dbt_cloud_unavailable: {exc}")
            self._available = False
        return self._available

    def list_semantic_objects(self, refresh: bool = False) -> List[SemanticObject]:
        """Enumerate all metrics, dimensions, entities from dbt Cloud Semantic Layer."""
        cache_ttl = 300
        if (
            not refresh
            and self._semantic_objects_cache is not None
            and (time.time() - self._cache_ts) < cache_ttl
        ):
            return self._semantic_objects_cache

        objects: List[SemanticObject] = []
        try:
            metrics = self._call_tool("list_metrics", {}) or []
            for m in self._parse_tool_result(metrics):
                objects.append(SemanticObject(
                    name=m.get("name", ""),
                    object_type="metric",
                    description=m.get("description", ""),
                    meta=m,
                ))

            dimensions = self._call_tool("list_dimensions", {"metrics": []}) or []
            for d in self._parse_tool_result(dimensions):
                name = d if isinstance(d, str) else d.get("name", str(d))
                objects.append(SemanticObject(
                    name=name,
                    object_type="dimension",
                    description=d.get("description", "") if isinstance(d, dict) else "",
                ))

            entities = self._call_tool("list_entities", {}) or []
            for e in self._parse_tool_result(entities):
                name = e if isinstance(e, str) else e.get("name", str(e))
                objects.append(SemanticObject(
                    name=name,
                    object_type="entity",
                ))

        except Exception as exc:
            logger.warning(f"dbt_cloud_list_failed: {exc}")

        self._semantic_objects_cache = objects
        self._cache_ts = time.time()
        return objects

    def compile_query(
        self,
        *,
        metrics: Optional[List[str]] = None,
        group_by: Optional[List[str]] = None,
        where: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> SemanticQueryResult:
        """Ask dbt Cloud to compile a semantic query into SQL."""
        t0 = time.perf_counter()
        params: Dict[str, Any] = {}
        if metrics:
            params["metrics"] = metrics
        if group_by:
            params["group_by"] = group_by
        if where:
            params["where"] = where
        if order_by:
            params["order_by"] = order_by
        if limit is not None:
            params["limit"] = limit

        try:
            result = self._call_tool("get_metrics_compiled_sql", params)
            compile_ms = (time.perf_counter() - t0) * 1000

            sql = ""
            objects_used = list(metrics or [])
            objects_used.extend(group_by or [])

            parsed = self._parse_tool_result(result)
            if isinstance(parsed, dict):
                sql = parsed.get("sql", parsed.get("compiled_sql", ""))
            elif isinstance(parsed, str):
                sql = parsed

            return SemanticQueryResult(
                sql=sql,
                semantic_objects_used=objects_used,
                compile_ms=round(compile_ms, 1),
                meta=parsed if isinstance(parsed, dict) else {"raw": parsed},
            )

        except Exception as exc:
            compile_ms = (time.perf_counter() - t0) * 1000
            logger.error(f"dbt_cloud_compile_failed: {exc}")
            return SemanticQueryResult(
                sql="",
                semantic_objects_used=[],
                compile_ms=round(compile_ms, 1),
                meta={"error": str(exc)},
            )

    def get_semantic_context_for_search(self) -> Dict[str, Any]:
        """Return a summary of available semantic context for explainability."""
        objects = self.list_semantic_objects()
        return {
            "metrics": [o.name for o in objects if o.object_type == "metric"],
            "dimensions": [o.name for o in objects if o.object_type == "dimension"],
            "entities": [o.name for o in objects if o.object_type == "entity"],
            "object_count": len(objects),
        }

    # ------------------------------------------------------------------
    # MCP HTTP transport
    # ------------------------------------------------------------------

    def _call_tool(self, tool_name: str, params: Dict[str, Any]) -> Any:
        """Call a tool on the dbt Cloud remote MCP endpoint via JSON-RPC."""
        url = f"{self._host}/api/ai/v1/mcp/"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": params,
            },
        }

        logger.info("dbt_cloud_call", extra={"extra": {
            "tool": tool_name,
            "params_keys": list(params.keys()),
        }})

        resp = httpx.post(
            url,
            json=payload,
            headers=self._headers(),
            timeout=_MCP_TIMEOUT,
        )
        resp.raise_for_status()

        body = resp.json()
        if "error" in body:
            raise RuntimeError(f"dbt MCP error: {body['error']}")

        return body.get("result", body)

    @staticmethod
    def _parse_tool_result(result: Any) -> Any:
        """Normalize the MCP tool result into a usable Python object."""
        if result is None:
            return []
        if isinstance(result, dict) and "content" in result:
            content = result["content"]
            if isinstance(content, list) and content:
                first = content[0]
                if isinstance(first, dict) and "text" in first:
                    try:
                        return json.loads(first["text"])
                    except (json.JSONDecodeError, TypeError):
                        return first["text"]
            return content
        if isinstance(result, str):
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                return result
        return result


_dbt_mcp_service: Optional[DbtMcpService] = None


def get_dbt_mcp_service() -> DbtMcpService:
    global _dbt_mcp_service
    if _dbt_mcp_service is None:
        _dbt_mcp_service = DbtMcpService()
    return _dbt_mcp_service
