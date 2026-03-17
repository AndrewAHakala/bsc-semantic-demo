-- =============================================================================
-- trace_log_tables.sql
-- Observability store — one row per API request.
-- =============================================================================

USE DATABASE global_supply_chain;
USE SCHEMA DEMO_BSC;

CREATE TABLE IF NOT EXISTS DEMO_TRACE_LOG (
    trace_id                     VARCHAR(36)     NOT NULL PRIMARY KEY,
    created_at                   TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    -- Request metadata
    mode                         VARCHAR(20),     -- structured | free_text
    normalized_request_summary   VARCHAR(1000),   -- redacted summary (no raw PII)

    -- Prompt versioning
    parse_prompt_version         VARCHAR(50),
    rerank_prompt_version        VARCHAR(50),

    -- SQL fingerprints
    candidate_sql_hash           VARCHAR(32),
    fetch_sql_hash               VARCHAR(32),

    -- Snowflake query IDs
    snowflake_qid_candidate      VARCHAR(100),
    snowflake_qid_fetch          VARCHAR(100),

    -- Result metadata
    candidate_count              INTEGER,
    chosen_order_ids             ARRAY,

    -- Latency breakdown (milliseconds)
    sql_candidate_ms             FLOAT,
    cortex_rerank_ms             FLOAT,
    sql_fetch_top_ms             FLOAT,
    total_ms                     FLOAT,

    -- Error (null on success)
    error                        VARCHAR(2000)
)
CLUSTER BY (created_at::DATE)
COMMENT = 'Per-request observability log for Order Status Assistant.'
;

-- Convenience view: last 7 days latency summary
CREATE OR REPLACE VIEW DEMO_TRACE_SUMMARY_V AS
SELECT
    DATE_TRUNC('hour', created_at)          AS hour_bucket,
    COUNT(*)                                AS request_count,
    MEDIAN(total_ms)                        AS p50_total_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP
        (ORDER BY total_ms)                 AS p95_total_ms,
    MEDIAN(sql_candidate_ms)                AS p50_candidate_ms,
    MEDIAN(cortex_rerank_ms)                AS p50_rerank_ms,
    SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) AS error_count
FROM DEMO_TRACE_LOG
WHERE created_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC
;

-- Grant INSERT to DEMO_ROLE now that the table exists
GRANT INSERT ON TABLE DEMO_BSC.DEMO_TRACE_LOG TO ROLE DEMO_ROLE;
