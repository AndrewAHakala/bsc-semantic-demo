"""Safe, parameterised Snowflake access layer.

All SQL comes from internal templates — never from user input.
Schema allowlist prevents reads outside DEMO_BSC.
"""

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import snowflake.connector

from api.core.config import settings
from api.core.errors import SchemaNotAllowedError
from api.core.log import get_logger

logger = get_logger(__name__)


@dataclass
class QueryResult:
    rows: List[Dict[str, Any]]
    query_id: str
    elapsed_ms: float
    sql_hash: str = ""


class SnowflakeService:
    """Singleton-friendly Snowflake connector wrapper."""

    def __init__(self):
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _connect(self) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            role=settings.snowflake_role,
            warehouse=settings.snowflake_warehouse,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            network_timeout=settings.query_timeout_s + 5,
            login_timeout=15,
        )

    def _get_conn(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None or self._conn.is_closed():
            self._conn = self._connect()
        return self._conn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        label: str = "query",
    ) -> QueryResult:
        """Execute an allowlisted, parameterised query and return rows + metadata."""
        self._assert_schema_safe(sql)
        sql_hash = hashlib.sha256(sql.encode()).hexdigest()[:16]
        params = params or {}

        for attempt in range(2):
            conn = self._get_conn()
            cur = conn.cursor(snowflake.connector.DictCursor)
            t0 = time.perf_counter()
            try:
                cur.execute(sql, params)
                rows = cur.fetchmany(settings.query_max_rows)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                query_id = cur.sfqid or ""
                logger.info(
                    "snowflake_query",
                    extra={
                        "extra": {
                            "label": label,
                            "query_id": query_id,
                            "rows": len(rows),
                            "elapsed_ms": round(elapsed_ms, 1),
                            "sql_hash": sql_hash,
                        }
                    },
                )
                return QueryResult(
                    rows=rows,
                    query_id=query_id,
                    elapsed_ms=round(elapsed_ms, 1),
                    sql_hash=sql_hash,
                )
            except snowflake.connector.errors.DatabaseError as exc:
                cur.close()
                if attempt == 0 and "Authentication token has expired" in str(exc):
                    logger.warning("snowflake_token_expired, reconnecting")
                    self._conn = None
                    continue
                logger.error(f"Snowflake query error [{label}]: {exc}")
                raise
            except snowflake.connector.errors.ProgrammingError as exc:
                logger.error(f"Snowflake query error [{label}]: {exc}")
                raise
            finally:
                cur.close()

    def healthcheck(self) -> bool:
        try:
            result = self.execute("SELECT 1 AS ok", label="healthcheck")
            return bool(result.rows)
        except Exception as exc:
            logger.error(f"Snowflake healthcheck failed: {exc}")
            return False

    # ------------------------------------------------------------------
    # Internal guards
    # ------------------------------------------------------------------

    def _assert_schema_safe(self, sql: str) -> None:
        """Reject queries that reference schemas outside the allowlist."""
        sql_upper = sql.upper()
        for token in ["DROP ", "DELETE ", "INSERT ", "UPDATE ", "TRUNCATE ", "ALTER ", "CREATE "]:
            if token in sql_upper:
                raise SchemaNotAllowedError(f"DML/DDL not allowed: {token.strip()}")

        import re
        allowed = {s.upper() for s in settings.allowed_schemas}
        exempt_dbs = {"SNOWFLAKE"}
        exempt_schemas = {"INFORMATION_SCHEMA", "CORTEX"}

        # Find complete dot-separated identifier chains (e.g. DB.SCHEMA.TABLE)
        # to avoid regex backtracking issues with partial matches.
        for chain in re.findall(
            r"[A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)+", sql_upper
        ):
            parts = chain.split(".")
            if len(parts) >= 3:
                db, schema = parts[0], parts[1]
                if db in exempt_dbs or schema in exempt_schemas:
                    continue
                if schema not in allowed:
                    raise SchemaNotAllowedError(schema)
            elif len(parts) == 2:
                schema = parts[0]
                if schema in exempt_dbs or schema in exempt_schemas:
                    continue
                if schema not in allowed:
                    raise SchemaNotAllowedError(schema)


# Module-level singleton
_snowflake_service: Optional[SnowflakeService] = None


def get_snowflake_service() -> SnowflakeService:
    global _snowflake_service
    if _snowflake_service is None:
        _snowflake_service = SnowflakeService()
    return _snowflake_service
