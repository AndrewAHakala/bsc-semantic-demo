"""Order Status Assistant — Streamlit UI.

Connects to the FastAPI backend.  Set API_BASE_URL in .env or the
environment to point at a non-local API server.
"""

import os
import json
import httpx
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from components.search_form import render_search_form
from components.results_table import render_results
from components.sql_panel import render_explain_panel
from components.trace_panel import render_trace_panel

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Order Status Assistant",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Session state init ─────────────────────────────────────────────────────
if "last_response" not in st.session_state:
    st.session_state.last_response = None
if "last_explain" not in st.session_state:
    st.session_state.last_explain = None
if "trace_history" not in st.session_state:
    st.session_state.trace_history = []

# ── Header ─────────────────────────────────────────────────────────────────
st.title("📦 Order Status Assistant")
st.markdown(
    "_Boston Scientific · Medical Device Fulfillment · "
    "Powered by Snowflake + Cortex_"
)

# Health check badge
try:
    hc = httpx.get(f"{API_BASE}/health", timeout=3.0)
    sf_ok = hc.json().get("snowflake", False)
    st.success("✅ Connected to Snowflake") if sf_ok else st.warning("⚠️ Snowflake degraded")
except Exception:
    st.error("❌ API unreachable — is the FastAPI server running?")

st.markdown("---")

# ── Main layout ─────────────────────────────────────────────────────────────
left_col, right_col = st.columns([1, 2])

with left_col:
    payload, run = render_search_form()

with right_col:
    if run:
        with st.spinner("Searching orders…"):
            try:
                resp = httpx.post(
                    f"{API_BASE}/search/orders",
                    json=payload,
                    timeout=30.0,
                )
                resp.raise_for_status()
                data = resp.json()
                st.session_state.last_response = data

                # Fetch explain data
                trace_id = data.get("trace_id", "")
                try:
                    ex_resp = httpx.get(
                        f"{API_BASE}/explain/{trace_id}", timeout=5.0
                    )
                    st.session_state.last_explain = ex_resp.json() if ex_resp.is_success else {}
                except Exception:
                    st.session_state.last_explain = {}

                # Append to history
                timings = data.get("timings_ms", {})
                st.session_state.trace_history.append(
                    {
                        "trace_id": trace_id[:8] + "…",
                        "total_ms": timings.get("total_ms", 0),
                        "sql_candidate_ms": timings.get("sql_candidate_ms", 0),
                        "cortex_rerank_ms": timings.get("cortex_rerank_ms", 0),
                        "sql_fetch_top_ms": timings.get("sql_fetch_top_ms", 0),
                    }
                )

            except httpx.HTTPStatusError as exc:
                st.error(f"API error {exc.response.status_code}: {exc.response.text}")
            except Exception as exc:
                st.error(f"Request failed: {exc}")

    if st.session_state.last_response:
        data = st.session_state.last_response
        results_tab, explain_tab, perf_tab = st.tabs(
            ["🔍 Results", "🧠 Explain", "⏱️ Performance"]
        )

        with results_tab:
            render_results(data.get("results", []), data.get("trace_id", ""))

        with explain_tab:
            # Merge SQL from search response into explain payload for display
            explain_data = st.session_state.last_explain or {}
            if not explain_data.get("candidate_sql") and data.get("candidate_sql"):
                explain_data["candidate_sql"] = data["candidate_sql"]
            if not explain_data.get("fetch_sql") and data.get("fetch_sql"):
                explain_data["fetch_sql"] = data["fetch_sql"]
            explain_data["candidate_count"] = data.get("candidate_count", "—")
            render_explain_panel(explain_data)

        with perf_tab:
            render_trace_panel(
                timings=data.get("timings_ms", {}),
                trace_id=data.get("trace_id", ""),
                history=st.session_state.trace_history,
            )
    else:
        st.info("Enter search criteria and click **Search Orders** to begin.")
