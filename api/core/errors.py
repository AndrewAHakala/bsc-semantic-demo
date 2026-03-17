from fastapi import Request
from fastapi.responses import JSONResponse


class SchemaNotAllowedError(Exception):
    def __init__(self, schema: str):
        super().__init__(f"Schema '{schema}' is not in the allowlist.")


class CortexError(Exception):
    pass


class CandidateCountExceededError(Exception):
    pass


class OrderNotFoundError(Exception):
    def __init__(self, order_id: str):
        super().__init__(f"Order '{order_id}' not found.")


async def schema_not_allowed_handler(request: Request, exc: SchemaNotAllowedError):
    return JSONResponse(status_code=403, content={"detail": str(exc)})


async def order_not_found_handler(request: Request, exc: OrderNotFoundError):
    return JSONResponse(status_code=404, content={"detail": str(exc)})


async def cortex_error_handler(request: Request, exc: CortexError):
    return JSONResponse(status_code=502, content={"detail": f"Cortex error: {exc}"})
