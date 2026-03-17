# Order Status Assistant

Customer-facing order lookup app for Boston Scientific medical device fulfillment.
Answers live-call questions like *"Where is my order?"*, *"Do you have tracking?"*, *"It was placed last week to St. Mary's."*

**Stack:** Streamlit · FastAPI · Snowflake · Snowflake Cortex · dbt Semantic Layer

---

## Architecture

```
┌───────────────┐        POST /search/orders        ┌─────────────────────┐
│  Streamlit UI │ ────────────────────────────────► │   FastAPI API        │
│  (port 8501)  │ ◄──────── SearchResponse ──────── │   (port 8000)        │
└───────────────┘                                   └──────────┬──────────┘
                                                               │
                         ┌─────────────────────────────────────┤
                         │          SemanticService            │
                         │                                     │
                         │  ┌─────────────┐ ┌──────────────┐  │
                         │  │DbtMcpService│ │CortexService │  │
                         │  │ (semantic   │ │ (rerank/parse)│  │
                         │  │  context)   │ │              │  │
                         │  └──────┬──────┘ └──────┬───────┘  │
                         │         │                │          │
                         │  ┌──────▼──────┐         │          │
                         │  │FuzzyService │◄────────┘          │
                         │  │ (SQL plan)  │                    │
                         │  └──────┬──────┘                    │
                         │         │                           │
                         │  ┌──────▼──────────────────────────┐│
                         │  │       SnowflakeService          ││
                         │  │  DEMO_BSC.ORDER_SEARCH_V        ││
                         │  │  DEMO_BSC.DEMO_TRACE_LOG        ││
                         │  └─────────────────────────────────┘│
                         └─────────────────────────────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │       dbt Project           │
                         │  staging → marts → semantic │
                         │  fct_orders · order_search_v│
                         └─────────────────────────────┘
```

### 3-step pipeline per request
1. **Deterministic SQL** — `FuzzyService` builds a parameterized LIKE query, Snowflake returns ≤ 200 candidates.
2. **Cortex Reranking** — `CortexService` calls `SNOWFLAKE.CORTEX.COMPLETE` with a compact JSON candidate list and the user query; returns ranked IDs + rationale.
3. **Final Fetch** — `SnowflakeService` fetches full `OrderStatusPayload` for top N IDs.

---

## Quick Start

### 1. Prerequisites
- Python 3.11+
- Snowflake account with Cortex enabled
- `.env` file (copy from `.env.example`)
- (Optional) dbt-core + dbt-snowflake for the semantic layer

### 2. One-command setup

```bash
cp .env.example .env  # fill in Snowflake credentials
./scripts/setup.sh
```

This will install dependencies, generate 100k synthetic orders, load them into Snowflake, and (if dbt is installed) run `dbt build`.

Flags: `--skip-data`, `--skip-dbt`, `--dry-run`.

### 3. Manual setup (alternative)

```bash
# DDL
snowsql -f infra/sql/create_schema.sql
snowsql -f infra/sql/create_tables.sql
snowsql -f infra/sql/create_views.sql
snowsql -f infra/sql/trace_log_tables.sql

# Synthetic data
cd infra/scripts && pip install -r requirements.txt
python generate_and_load.py --orders 100000

# dbt (optional, requires dbt-core + dbt-snowflake)
cd dbt && dbt deps --profiles-dir . && dbt build --profiles-dir .
```

### 4. Run locally

```bash
# Terminal 1 — API
pip install -r api/requirements.txt
uvicorn api.main:app --reload

# Terminal 2 — UI
cd ui && pip install -r requirements.txt
streamlit run app.py
```

Open **http://localhost:8501** for the UI, **http://localhost:8000/docs** for the API.

### 5. Docker Compose

```bash
docker compose up --build
```

---

## dbt Semantic Layer

The `dbt/` directory contains a full dbt project that models the synthetic data:

```
dbt/
  dbt_project.yml
  profiles.yml
  packages.yml
  macros/
    normalize_text.sql      # Snowflake-compatible text normalization
    search_blob.sql         # Concatenated search field builder
  models/
    staging/                # Raw → cleaned (views)
      stg_customers.sql
      stg_facilities.sql
      stg_orders.sql
      stg_order_items.sql
      stg_products.sql
      stg_contacts.sql
    marts/orders/           # Canonical entities (tables)
      fct_orders.sql        # Denormalized order fact
      order_search_v.sql    # Search-optimized view (replaces SQL-only ORDER_SEARCH_V)
    semantic_models/        # MetricFlow semantic definitions
      sem_orders.yml        # Metrics: order_volume, revenue, fulfillment_rate, etc.
      sem_order_items.yml   # Metrics: line_item_count, units_ordered, etc.
```

### Semantic objects defined

| Type | Name | Description |
|------|------|-------------|
| Metric | `order_volume` | Total number of orders |
| Metric | `revenue` | Total order revenue USD |
| Metric | `average_order_value` | Avg order value USD |
| Metric | `fulfillment_rate` | % shipped/delivered |
| Metric | `priority_rate` | % priority-flagged |
| Dimension | `status` | Order fulfillment status |
| Dimension | `customer_name` | Customer account name |
| Dimension | `facility_name` | Shipping facility name |
| Dimension | `sales_region` | Geographic sales region |
| Entity | `order` | Primary: order_id |
| Entity | `customer` | Foreign: customer_account_id |
| Entity | `facility` | Foreign: facility_id |

### dbt MCP integration

The API supports a `SEMANTIC_BACKEND` setting (`dbt_mcp` or `direct_sql`):

- **`dbt_mcp`** (default): On startup, the `DbtMcpService` checks for a running dbt MCP server. If available, it enumerates semantic objects and uses them for governed context in explainability panels. If unavailable, falls back gracefully to `direct_sql`.
- **`direct_sql`**: Uses the SQL views directly without MCP.

The dbt MCP server must be configured separately in Cursor settings. The `DbtMcpService` logs all semantic object references per request for full traceability.

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/search/orders` | Main order lookup — structured or free-text |
| `GET`  | `/orders/{order_id}` | Direct single-order status fetch |
| `GET`  | `/explain/{trace_id}` | Full explainability artifact for a request |
| `GET`  | `/health` | Snowflake connectivity check |

### SearchRequest (POST /search/orders)

```json
{
  "mode": "free_text",
  "free_text": "St Mary's order from last Tuesday — can you find it?",
  "fields": {},
  "top_n": 5
}
```

Or structured:

```json
{
  "mode": "structured",
  "fields": {
    "facility_name": "Cleveland Clinic",
    "date_start": "2026-03-01",
    "date_end": "2026-03-15"
  },
  "top_n": 5
}
```

### SearchResponse

```json
{
  "trace_id": "abc123",
  "results": [
    {
      "order_id": "SO-2026-000123",
      "purchase_order_id": "PO-00012-884192",
      "status": "SHIPPED",
      "status_last_updated_ts": "2026-03-10T14:22:00Z",
      "customer_name": "St. Mary's Hospital",
      "facility_name": "St Marys Hosp - Boston",
      "promised_delivery_date": "2026-03-12",
      "carrier": "UPS",
      "tracking_number": "1Z...",
      "match_score": 0.92,
      "match_reasons": ["facility token match", "date window match"]
    }
  ],
  "timings_ms": {
    "sql_candidate_ms": 480,
    "cortex_rerank_ms": 950,
    "sql_fetch_top_ms": 120,
    "total_ms": 1700
  },
  "candidate_count": 47,
  "candidate_sql": "...",
  "fetch_sql": "..."
}
```

### ExplainResponse (GET /explain/{trace_id})

Returns: candidate SQL, candidate count, pre-rerank candidates, rerank rationale, fetch SQL, timings, prompt versions, Snowflake query IDs, semantic objects used, and semantic backend identifier.

---

## Evaluation Harness

```bash
pip install httpx
python evaluation/run_eval.py --api-url http://localhost:8000 --output results.json
python evaluation/report.py results.json
```

Outputs accuracy@5, p50, p95 latency, and a per-prompt diff report.

15 golden prompts cover exact ID lookups, fuzzy facility matching, date window queries, PO lookups, and status-filtered searches.

---

## Observability

Every request produces a trace record in `DEMO_BSC.DEMO_TRACE_LOG`:

| Column | Description |
|--------|-------------|
| `trace_id` | UUID per request |
| `snowflake_qid_candidate` | Snowflake query ID for candidate retrieval |
| `snowflake_qid_fetch` | Snowflake query ID for final fetch |
| `sql_candidate_ms` | Candidate query latency |
| `cortex_rerank_ms` | Cortex rerank latency |
| `sql_fetch_top_ms` | Final fetch latency |
| `total_ms` | End-to-end latency |
| `parse_prompt_version` | Version of the parse prompt template |
| `rerank_prompt_version` | Version of the rerank prompt template |

`DEMO_TRACE_SUMMARY_V` provides hourly p50/p95 latency rollups for the last 7 days.

---

## Security Model (demo)

- Snowflake service account is **read-only** on `DEMO_BSC.*`
- All queries are **parameterized templates** — no user SQL accepted
- Schema **allowlist** enforced in `SnowflakeService` (validates both 2-part and 3-part references)
- DML/DDL blocked: DROP, DELETE, INSERT, UPDATE, TRUNCATE, ALTER, CREATE
- Row limits and query timeouts enforced per request
- Trace log writes bypass the safety check via direct connection (INSERT-only to `DEMO_TRACE_LOG`)

---

## Repository Structure

```
bsc-semantic-demo/
  README.md
  docker-compose.yml
  .env.example
  scripts/setup.sh              # One-command setup

  api/
    main.py                     # FastAPI app
    core/                       # config, logging, timing, errors
    services/
      semantic_service.py       # Orchestration — stable contract
      fuzzy_service.py          # Deterministic SQL candidate builder
      cortex_service.py         # Cortex parse + rerank
      snowflake_service.py      # Parameterized, allowlisted Snowflake access
      dbt_mcp_service.py        # dbt Semantic Layer MCP adapter
      explain_service.py        # Explainability packaging
    schemas/                    # Pydantic models (domain, search, explain, trace)
    routers/                    # FastAPI route handlers + DI

  ui/
    app.py                      # Streamlit entrypoint
    components/                 # search_form, results_table, sql_panel, trace_panel

  dbt/
    dbt_project.yml
    profiles.yml
    packages.yml
    macros/                     # normalize_text, search_blob
    models/
      staging/                  # stg_customers, stg_facilities, stg_orders, etc.
      marts/orders/             # fct_orders, order_search_v
      semantic_models/          # sem_orders.yml, sem_order_items.yml

  infra/
    sql/                        # create_schema, create_tables, create_views, trace_log
    scripts/generate_and_load.py
    dataset/README_DATASET.md

  evaluation/
    datasets/golden_prompts.jsonl
    datasets/expected_results.jsonl
    run_eval.py
    report.py
```

---

## Performance SLO

| Metric | Target | Notes |
|--------|--------|-------|
| p95 end-to-end | ≤ 5,000 ms | Full pipeline |
| Candidate SQL | ≤ 500–1,000 ms | Date filter + LIKE on norm columns |
| Cortex Rerank | ≤ 2,000 ms | ≤ 200 candidates, compact payload |
| Final fetch | ≤ 200 ms | Top 5 order_ids by PK |

Rerank results are cached for 10 minutes (TTL configurable) — identical queries on live calls return instantly.

---

## Extending for Future Clients

`SemanticService` is the stable contract. To add Agentforce or Tableau Next:

1. Add a new FastAPI router (or a separate service) that calls `SemanticService.search_orders(request)`.
2. Translate the client's query format into a `SearchRequest`.
3. No changes to core logic, SQL, or Cortex prompts required.

The `DbtMcpService` provides governed semantic context that future clients can leverage for metric-aware queries.
