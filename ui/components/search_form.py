"""Search input panel — structured and free-text modes."""

import streamlit as st
from datetime import date, timedelta


def render_search_form() -> dict:
    """Render the search form and return a SearchRequest-compatible dict."""
    st.subheader("Order Lookup")

    mode = st.radio(
        "Search mode",
        ["structured", "free_text"],
        format_func=lambda m: "Structured Fields" if m == "structured" else "Free Text",
        horizontal=True,
        key="search_mode",
    )

    payload = {"mode": mode, "fields": {}}

    if mode == "free_text":
        free_text = st.text_area(
            "Describe the order",
            placeholder=(
                "e.g. St Mary's order from last Tuesday — can you find it?\n"
                "Need tracking for Cleveland Clinic order placed early March."
            ),
            height=100,
            key="free_text_input",
        )
        payload["free_text"] = free_text
        payload["fields"] = {}
    else:
        col1, col2 = st.columns(2)
        with col1:
            payload["fields"]["order_id"] = st.text_input(
                "Order ID", placeholder="SO-2026-001234", key="order_id"
            ) or None
            payload["fields"]["purchase_order_id"] = st.text_input(
                "Purchase Order ID", placeholder="PO-884192", key="po_id"
            ) or None
            payload["fields"]["customer_name"] = st.text_input(
                "Customer / Account Name", placeholder="Boston Scientific", key="cust_name"
            ) or None
        with col2:
            payload["fields"]["facility_name"] = st.text_input(
                "Facility Name", placeholder="St. Mary's Hospital", key="facility"
            ) or None
            payload["fields"]["contact_name"] = st.text_input(
                "Contact Name", placeholder="Jane Smith", key="contact"
            ) or None

            default_start = date.today() - timedelta(days=30)
            date_range = st.date_input(
                "Order date range",
                value=(default_start, date.today()),
                key="date_range",
            )
            if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
                payload["fields"]["date_start"] = str(date_range[0])
                payload["fields"]["date_end"] = str(date_range[1])

    top_n = st.slider("Max results to return", min_value=1, max_value=10, value=5, key="top_n")
    payload["top_n"] = top_n

    run = st.button("Search Orders", type="primary", use_container_width=True)
    return payload, run
