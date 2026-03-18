"""Metric results panel — table display for Semantic Layer query results."""

import streamlit as st
import pandas as pd
from typing import Dict, Any, Optional


def render_metric_results(metric_result: Dict[str, Any], trace_id: str) -> None:
    if not metric_result:
        st.info("No metric data returned.")
        return

    rows = metric_result.get("rows", [])
    columns = metric_result.get("columns", [])
    metrics_used = metric_result.get("metrics_used", [])
    dimensions_used = metric_result.get("dimensions_used", [])

    st.subheader("Metric Results")
    st.caption(f"Trace ID: `{trace_id}`")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Metrics:** {', '.join(f'`{m}`' for m in metrics_used)}")
    with col2:
        if dimensions_used:
            st.markdown(f"**Grouped by:** {', '.join(f'`{d}`' for d in dimensions_used)}")

    if not rows:
        st.warning("The query returned no data.")
        return

    df = pd.DataFrame(rows)

    # Clean column names for display
    display_cols = {}
    for col in df.columns:
        clean = col.replace("__", " ").replace("_", " ").title()
        display_cols[col] = clean
    df = df.rename(columns=display_cols)

    st.dataframe(df, use_container_width=True, hide_index=True)

    # Bar chart for numeric columns if there's a grouping dimension
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    non_numeric_cols = [c for c in df.columns if c not in numeric_cols]

    if numeric_cols and non_numeric_cols and len(df) > 1:
        chart_metric = numeric_cols[0]
        chart_dim = non_numeric_cols[0]
        chart_df = df[[chart_dim, chart_metric]].set_index(chart_dim)
        st.bar_chart(chart_df)

    # Show compiled SQL
    compiled_sql = metric_result.get("compiled_sql")
    if compiled_sql:
        with st.expander("Compiled SQL (from dbt Semantic Layer)"):
            st.code(compiled_sql, language="sql")

    st.markdown(
        f"**{metric_result.get('row_count', 0)} rows** returned · "
        f"Powered by **dbt Semantic Layer**"
    )
