from pydantic_settings import BaseSettings
from typing import List, Literal


class Settings(BaseSettings):
    # Snowflake
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_role: str = "DEMO_ROLE"
    snowflake_warehouse: str = "DEMO_WH"
    snowflake_database: str = "DEMO_DB"
    snowflake_schema: str = "DEMO_BSC"

    # Cortex
    cortex_model: str = "mistral-7b"
    cortex_timeout_s: int = 30

    # Service tuning
    max_candidates: int = 200
    default_top_n: int = 5
    rerank_cache_ttl_s: int = 600

    # Query limits
    query_max_rows: int = 500
    query_timeout_s: int = 10

    # Allowed schemas (allowlist)
    allowed_schemas: List[str] = ["DEMO_BSC"]

    # Prompt versioning
    parse_prompt_version: str = "parse-v1"
    rerank_prompt_version: str = "rerank-v1"

    # Semantic backend: 'dbt_mcp' uses dbt Cloud Semantic Layer; 'direct_sql' bypasses it
    semantic_backend: Literal["dbt_mcp", "direct_sql"] = "dbt_mcp"

    # dbt Cloud connection (required when semantic_backend = 'dbt_mcp')
    dbt_cloud_host: str = ""            # e.g. https://cloud.getdbt.com
    dbt_cloud_token: str = ""           # Personal Access Token or service token
    dbt_cloud_environment_id: str = ""  # Production environment ID

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
