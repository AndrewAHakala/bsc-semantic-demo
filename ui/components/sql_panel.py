"""Explain panel — SQL, rerank rationale, semantic objects used."""

import streamlit as st
from typing import Dict, Any, Optional


def render_explain_panel(explain: Dict[str, Any]) -> None:
    if not explain:
        return

    st.subheader("Explain")
    tabs = st.tabs(["Candidate SQL", "Rerank", "Final Fetch SQL", "Normalized Request"])

    with tabs[0]:
        st.markdown(
            f"**Candidates retrieved:** {explain.get('candidate_count', '—')}"
        )
        st.code(explain.get("candidate_sql", ""), language="sql")
        qids = explain.get("snowflake_query_ids", {})
        if qids.get("candidate"):
            st.caption(f"Snowflake Query ID: `{qids['candidate']}`")

    with tabs[1]:
        rerank_order = explain.get("rerank_order", [])
        rationale = explain.get("rerank_rationale", {})
        if rerank_order:
            st.markdown("**Reranked order:**")
            for i, oid in enumerate(rerank_order, 1):
                reason = rationale.get(oid, "")
                st.markdown(f"{i}. `{oid}` — {reason}")
        else:
            st.info("Reranking skipped (exact match or empty candidate set).")

        versions = explain.get("prompt_versions", {})
        if versions:
            st.caption(
                f"Prompt versions — parse: `{versions.get('parse','—')}` "
                f"· rerank: `{versions.get('rerank','—')}`"
            )

        if explain.get("rerank_prompt_used"):
            with st.expander("Raw rerank prompt"):
                st.text(explain["rerank_prompt_used"][:3000])

    with tabs[2]:
        st.code(explain.get("fetch_sql", ""), language="sql")
        if qids.get("fetch_top"):
            st.caption(f"Snowflake Query ID: `{qids['fetch_top']}`")

    with tabs[3]:
        nr = explain.get("normalized_request")
        if nr:
            import json
            st.json(json.dumps(nr, default=str))
        else:
            st.info("No normalized request data available.")

        sem_backend = explain.get("semantic_backend")
        sem_objects = explain.get("semantic_objects_used", [])
        if sem_backend:
            st.markdown(f"**Semantic backend:** `{sem_backend}`")
        if sem_objects:
            st.markdown("**Semantic objects used:**")
            for obj in sem_objects:
                st.markdown(f"  - `{obj}`")
