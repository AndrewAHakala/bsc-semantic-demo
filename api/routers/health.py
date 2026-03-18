from fastapi import APIRouter, Depends
from api.core.config import settings
from api.services.snowflake_service import SnowflakeService, get_snowflake_service
from api.services.dbt_mcp_service import get_dbt_mcp_service

router = APIRouter()


@router.get("/health", tags=["ops"])
def health(sf: SnowflakeService = Depends(get_snowflake_service)):
    sf_ok = sf.healthcheck()

    dbt_cloud_ok = False
    dbt_cloud_configured = bool(
        settings.dbt_cloud_host and settings.dbt_cloud_token and settings.dbt_cloud_environment_id
    )
    if dbt_cloud_configured:
        try:
            svc = get_dbt_mcp_service()
            dbt_cloud_ok = svc.is_available
            if not dbt_cloud_ok:
                dbt_cloud_ok = svc.check_availability()
        except Exception:
            dbt_cloud_ok = False

    status = "ok" if sf_ok else "degraded"
    return {
        "status": status,
        "snowflake": sf_ok,
        "dbt_cloud": dbt_cloud_ok,
        "dbt_cloud_configured": dbt_cloud_configured,
        "semantic_backend": settings.semantic_backend,
    }
