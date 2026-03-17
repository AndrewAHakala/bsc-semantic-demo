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

    # Current request timings
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Candidate SQL", f"{timings.get('sql_candidate_ms', 0):.0f} ms")
    col2.metric("Cortex Rerank", f"{timings.get('cortex_rerank_ms', 0):.0f} ms")
    col3.metric("Final Fetch", f"{timings.get('sql_fetch_top_ms', 0):.0f} ms")
    col4.metric(
        "Total",
        f"{timings.get('total_ms', 0):.0f} ms",
        delta=(
            f"{'✅ under 5s' if timings.get('total_ms', 0) < 5000 else '⚠️ over SLO'}"
        ),
        delta_color="normal",
    )

    st.caption(f"Trace ID: `{trace_id}`")

    # History table
    if history:
        st.markdown("---")
        st.markdown("**Last requests**")
        df = pd.DataFrame(history)
        display_cols = [
            c for c in ["trace_id", "total_ms", "sql_candidate_ms", "cortex_rerank_ms", "sql_fetch_top_ms"]
            if c in df.columns
        ]
        st.dataframe(df[display_cols].tail(20), use_container_width=True, hide_index=True)

        # p50 / p95
        if "total_ms" in df.columns and len(df) >= 2:
            p50 = df["total_ms"].quantile(0.50)
            p95 = df["total_ms"].quantile(0.95)
            st.markdown(
                f"p50 latency: **{p50:.0f} ms** · p95 latency: **{p95:.0f} ms** "
                f"{'✅' if p95 < 5000 else '⚠️ exceeds 5 s SLO'}"
            )
