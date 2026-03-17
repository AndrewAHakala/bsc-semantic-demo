-- =============================================================================
-- create_views.sql
-- ORDER_SEARCH_V — the single denormalized view the API queries.
--
-- Adds:
--   customer_name_norm  — for LIKE token matching
--   facility_name_norm  — for LIKE token matching
--   search_blob         — concatenated normalized text for fallback token scan
-- =============================================================================

USE DATABASE global_supply_chain;
USE SCHEMA DEMO_BSC;

CREATE OR REPLACE VIEW ORDER_SEARCH_V
    COMMENT = 'Denormalized order search view. Read-only. Used by Order Status Assistant API.'
AS
SELECT
    o.order_id,
    o.purchase_order_id,
    o.status,
    o.status_last_updated_ts,

    -- Customer
    c.customer_name,
    c.customer_name_norm,

    -- Facility (include alternate name for broader matching)
    f.facility_name,
    f.facility_name_norm,
    f.facility_name_alt,
    f.city,
    f.state,
    f.zip,

    -- Dates
    o.order_created_ts,
    o.requested_ship_date,
    o.promised_delivery_date,
    o.actual_ship_ts,
    o.actual_delivery_date,

    -- Fulfillment
    o.carrier,
    o.tracking_number,
    o.priority_flag,

    -- Financial
    o.total_amount_usd,
    o.currency,
    o.sales_region,

    -- Searchable blob — lower-cased concatenation for fallback token matching
    LOWER(
        COALESCE(o.order_id, '') || ' ' ||
        COALESCE(o.purchase_order_id, '') || ' ' ||
        COALESCE(c.customer_name_norm, '') || ' ' ||
        COALESCE(f.facility_name_norm, '') || ' ' ||
        COALESCE(f.facility_name_alt, '') || ' ' ||
        COALESCE(f.city, '') || ' ' ||
        COALESCE(f.state, '') || ' ' ||
        COALESCE(f.zip, '')
    ) AS search_blob

FROM ORDER_FACT          o
JOIN CUSTOMER_DIM        c ON c.customer_account_id = o.customer_account_id
JOIN FACILITY_DIM        f ON f.facility_id         = o.facility_id
;
