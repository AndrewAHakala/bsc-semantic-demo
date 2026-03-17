"""FastAPI dependency injection — service singletons."""

from functools import lru_cache
from api.core.config import settings
from api.services.snowflake_service import get_snowflake_service
from api.services.cortex_service import CortexService
from api.services.fuzzy_service import FuzzyService
from api.services.explain_service import ExplainService
from api.services.dbt_mcp_service import DbtMcpService, get_dbt_mcp_service
from api.services.semantic_service import SemanticService


@lru_cache(maxsize=1)
def get_semantic_service() -> SemanticService:
    sf = get_snowflake_service()
    cortex = CortexService(sf)
    fuzzy = FuzzyService()
    explain = ExplainService()
    dbt_mcp = get_dbt_mcp_service() if settings.semantic_backend == "dbt_mcp" else None
    return SemanticService(
        snowflake=sf,
        cortex=cortex,
        fuzzy=fuzzy,
        explain=explain,
        dbt_mcp=dbt_mcp,
    )
