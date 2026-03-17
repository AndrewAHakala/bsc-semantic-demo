"""Order Status Assistant — FastAPI application entry point.

Stable service interface designed for reuse by:
  - Streamlit UI (current)
  - Agentforce (future)
  - Tableau Next (future)
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.core.errors import (
    CortexError,
    OrderNotFoundError,
    SchemaNotAllowedError,
    cortex_error_handler,
    order_not_found_handler,
    schema_not_allowed_handler,
)
from api.routers import health, search, explain

app = FastAPI(
    title="Order Status Assistant",
    description=(
        "Customer-facing order lookup service backed by Snowflake + Cortex. "
        "Deterministic SQL candidate retrieval → Cortex reranking → status payload."
    ),
    version="1.0.0",
)

# CORS — allow Streamlit UI and future clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Exception handlers
app.add_exception_handler(SchemaNotAllowedError, schema_not_allowed_handler)
app.add_exception_handler(OrderNotFoundError, order_not_found_handler)
app.add_exception_handler(CortexError, cortex_error_handler)

# Routers
app.include_router(health.router)
app.include_router(search.router)
app.include_router(explain.router)
