"""Results panel — top matched orders table + detail view."""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any

STATUS_COLORS = {
    "DELIVERED": "🟢",
    "SHIPPED": "🔵",
    "PICKED": "🟡",
    "ALLOCATED": "🟡",
    "CREATED": "⚪",
    "BACKORDERED": "🔴",
    "ON_HOLD": "🟠",
    "CANCELLED": "⛔",
}


def _status_icon(status: str) -> str:
    return STATUS_COLORS.get(status.upper(), "⚪")


def render_results(results: List[Dict[str, Any]], trace_id: str) -> None:
    if not results:
        st.info("No matching orders found.")
        return

    st.subheader(f"Top {len(results)} Matches  ·  `trace_id: {trace_id}`")

    # Summary table
    rows = []
    for r in results:
        rows.append(
            {
                "": _status_icon(r.get("status", "")),
                "Order ID": r.get("order_id", ""),
                "PO ID": r.get("purchase_order_id", "") or "—",
                "Status": r.get("status", ""),
                "Customer": r.get("customer_name", ""),
                "Facility": r.get("facility_name", ""),
                "Promised Delivery": str(r.get("promised_delivery_date") or "—"),
                "Tracking": r.get("tracking_number") or "—",
                "Score": f"{r.get('match_score', 0):.2f}",
            }
        )
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Detail expanders per result
    st.markdown("---")
    for i, r in enumerate(results):
        with st.expander(
            f"{_status_icon(r.get('status',''))} {r.get('order_id','')} — {r.get('facility_name','')}",
            expanded=(i == 0),
        ):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f"**Status:** `{r.get('status','')}`")
                st.markdown(f"**Customer:** {r.get('customer_name','')}")
                st.markdown(f"**Facility:** {r.get('facility_name','')}")
                st.markdown(f"**Purchase Order:** {r.get('purchase_order_id') or '—'}")
                st.markdown(f"**Region:** {r.get('sales_region') or '—'}")
            with c2:
                st.markdown(f"**Promised Delivery:** {r.get('promised_delivery_date') or '—'}")
                st.markdown(f"**Requested Ship:** {r.get('requested_ship_date') or '—'}")
                st.markdown(f"**Actual Ship:** {r.get('actual_ship_ts') or '—'}")
                st.markdown(f"**Carrier:** {r.get('carrier') or '—'}")
                st.markdown(f"**Tracking #:** `{r.get('tracking_number') or '—'}`")

            st.markdown(f"**Status Updated:** {r.get('status_last_updated_ts','')}")
            if r.get("priority_flag"):
                st.warning("⚡ Priority Order")
            if r.get("total_amount_usd"):
                st.markdown(f"**Order Value:** ${r['total_amount_usd']:,.2f} {r.get('currency','USD')}")

            reasons = r.get("match_reasons", [])
            if reasons:
                st.markdown("**Match Reasons:**")
                for reason in reasons:
                    st.markdown(f"  - {reason}")
