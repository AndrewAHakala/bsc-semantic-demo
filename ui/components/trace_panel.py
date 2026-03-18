"""Performance / trace panel — timings, latency history, p50/p95."""

import streamlit as st
import pandas as pd
from typing import Dict, Any, List


def render_trace_panel(
    timings: Dict[str, float],
    trace_id: str,
    history: List[Dict[str, Any]],
) -> None:
    st.subheader("Performance")

    total_ms = timings.get("total_ms", 0)
    cortex_parse = timings.get("cortex_parse_ms", 0)
    mcp_query = timings.get("mcp_query_ms", 0)
    sql_cand = timings.get("sql_candidate_ms", 0)
    cortex_rerank = timings.get("cortex_rerank_ms", 0)
    sql_fetch = timings.get("sql_fetch_top_ms", 0)

    # Show the relevant metrics based on what has values
    cols = st.columns(5)
    idx = 0
    if cortex_parse > 0:
        cols[idx].metric("Cortex Parse", f"{cortex_parse:.0f} ms")
        idx += 1
    if mcp_query > 0:
        cols[idx].metric("MCP Query", f"{mcp_query:.0f} ms")
        idx += 1
    if sql_cand > 0:
        cols[idx].metric("Candidate SQL", f"{sql_cand:.0f} ms")
        idx += 1
    if cortex_rerank > 0:
        cols[idx].metric("Cortex Rerank", f"{cortex_rerank:.0f} ms")
        idx += 1
    if sql_fetch > 0:
        cols[idx].metric("Final Fetch", f"{sql_fetch:.0f} ms")
        idx += 1

    slo_label = "under 5s" if total_ms < 5000 else "over SLO"
    st.metric(
        "Total End-to-End",
        f"{total_ms:.0f} ms",
        delta=slo_label,
        delta_color="normal" if total_ms < 5000 else "inverse",
    )

    st.caption(f"Trace ID: `{trace_id}`")

    # History table
    if history:
        st.markdown("---")
        st.markdown("**Request history**")
        df = pd.DataFrame(history)
        display_cols = [
            c for c in [
                "trace_id", "type", "total_ms", "cortex_parse_ms",
                "mcp_query_ms", "sql_candidate_ms", "cortex_rerank_ms", "sql_fetch_top_ms",
            ]
            if c in df.columns
        ]
        st.dataframe(df[display_cols].tail(20), use_container_width=True, hide_index=True)

        if "total_ms" in df.columns and len(df) >= 2:
            p50 = df["total_ms"].quantile(0.50)
            p95 = df["total_ms"].quantile(0.95)
            st.markdown(
                f"p50 latency: **{p50:.0f} ms** · p95 latency: **{p95:.0f} ms** "
                f"{'✅' if p95 < 5000 else '⚠️ exceeds 5 s SLO'}"
            )
