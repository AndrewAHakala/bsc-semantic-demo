from fastapi import APIRouter, Depends
from api.services.snowflake_service import SnowflakeService, get_snowflake_service

router = APIRouter()


@router.get("/health", tags=["ops"])
def health(sf: SnowflakeService = Depends(get_snowflake_service)):
    sf_ok = sf.healthcheck()
    status = "ok" if sf_ok else "degraded"
    return {"status": status, "snowflake": sf_ok}
