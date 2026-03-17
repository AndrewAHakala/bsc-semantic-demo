from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date


class OrderStatusPayload(BaseModel):
    """Canonical customer-facing order status object.

    Required fields are always populated.  Optional fields are null when not yet
    available (e.g., tracking_number before ship).  Add new fields here and
    they propagate to all API responses and future clients automatically.
    """

    # Required
    order_id: str
    status: str
    status_last_updated_ts: datetime
    customer_name: str
    facility_name: str

    # Strongly recommended
    promised_delivery_date: Optional[date] = None

    # Nice-to-have (nullable)
    carrier: Optional[str] = None
    tracking_number: Optional[str] = None
    actual_ship_ts: Optional[datetime] = None
    actual_delivery_date: Optional[date] = None
    priority_flag: Optional[bool] = None
    purchase_order_id: Optional[str] = None
    requested_ship_date: Optional[date] = None
    total_amount_usd: Optional[float] = None
    currency: Optional[str] = None
    sales_region: Optional[str] = None


class CandidateSummary(BaseModel):
    """Compact projection sent to Cortex for reranking.

    Keep this small — it's serialised into the LLM prompt.
    """

    order_id: str
    purchase_order_id: Optional[str] = None
    status: str
    status_last_updated_ts: datetime
    customer_name: str
    facility_name: str
    promised_delivery_date: Optional[date] = None
    tracking_number: Optional[str] = None
    score: float = 0.0
