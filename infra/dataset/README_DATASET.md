# BSC Order Status Assistant — Synthetic Dataset

## Purpose
This dataset simulates Boston Scientific's medical device order fulfillment
environment. It is designed for demo and evaluation purposes only.

---

## Schema: `DEMO_BSC`

### Tables

| Table | Grain | Volume |
|---|---|---|
| `CUSTOMER_DIM` | 1 row per customer account | ~500 |
| `FACILITY_DIM` | 1 row per shipping facility | ~1,200 |
| `PRODUCT_DIM` | 1 row per product SKU | ~350 |
| `CONTACT_DIM` | 1 row per contact person | ~800 |
| `ORDER_FACT` | 1 row per sales order | ~100,000 |
| `ORDER_ITEM_FACT` | 1 row per order line item | ~180,000 |

### View

| View | Description |
|---|---|
| `ORDER_SEARCH_V` | Denormalized join of ORDER_FACT + CUSTOMER_DIM + FACILITY_DIM. Primary query target for the API. Includes `search_blob` for fallback token matching. |
| `DEMO_TRACE_SUMMARY_V` | Hourly p50/p95 latency rollup from `DEMO_TRACE_LOG`. |

---

## Key Columns

### ORDER_FACT
| Column | Notes |
|---|---|
| `order_id` | Format: `SO-{YYYY}-{NNNNNN}` |
| `purchase_order_id` | Customer-assigned PO number, format: `PO-{NNNNN}-{NNNNNN}` |
| `status` | `CREATED \| ALLOCATED \| PICKED \| SHIPPED \| DELIVERED \| BACKORDERED \| CANCELLED \| ON_HOLD` |
| `tracking_number` | NULL until status ≥ SHIPPED |
| `priority_flag` | ~8% of orders |

### FACILITY_DIM
| Column | Notes |
|---|---|
| `facility_name` | Human-readable name, e.g. "St. Mary's General Hospital" |
| `facility_name_norm` | Lowercased, ASCII, punctuation stripped — used for LIKE matching |
| `facility_name_alt` | Common abbreviation variant, e.g. "St Marys Gen Hosp" |

---

## Fuzzy Realism

The dataset is specifically designed to stress-test fuzzy matching:

1. **Near-duplicate facility names** — "St. Mary's Hospital" vs "Saint Mary Hospital" vs "St Marys Hosp"
2. **Common abbreviations** — "Med Ctr" ↔ "Medical Center", "Reg" ↔ "Regional"
3. **Partial order IDs** — callers often provide only the last 4-5 digits
4. **Missing tracking** — pre-ship orders have `tracking_number = NULL`
5. **Zipf distribution** — top 10% of facilities generate ~60% of orders
6. **Date window queries** — orders span 2024-01-01 to present

---

## Example Prompts → Field Mapping

| Prompt | Extracted Fields |
|---|---|
| `"Order 01234 is late. Latest status?"` | `order_id` partial match → `SO-*-001234` |
| `"St Mary's order from last Tuesday"` | `facility_name ≈ "St Mary"`, `date_start/end` around last Tuesday |
| `"Need tracking for Cleveland Clinic order placed early March"` | `facility_name ≈ "Cleveland Clinic"`, `date_start=2026-03-01`, `date_end=2026-03-10` |
| `"Boston Scientific PO 884192"` | `purchase_order_id ≈ "884192"` |
| `"Late shipment to Mercy Chicago, contact is Jane Smith"` | `facility_name ≈ "Mercy Chicago"`, `contact_name ≈ "Jane Smith"` |

---

## Distribution Notes

- `status` follows a realistic fulfillment funnel: ~50% DELIVERED, ~20% SHIPPED, ~8% PICKED/ALLOCATED
- ~4% are BACKORDERED — common topic for customer calls
- Orders span `2024-01-01` through ~`2026-03-15`
- ~8% marked `priority_flag = TRUE`
