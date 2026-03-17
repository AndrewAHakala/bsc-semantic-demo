{{
    config(
        materialized='view'
    )
}}

{#-
ORDER_SEARCH_V — the denormalized search view used by the Order Status Assistant API.
Includes normalized text fields and a concatenated search_blob for fallback token matching.
This view is the primary query target for candidate retrieval.
-#}

select
    order_id,
    purchase_order_id,
    status,
    status_last_updated_ts,

    customer_name,
    customer_name_norm,

    facility_name,
    facility_name_norm,
    facility_name_alt,
    facility_city  as city,
    facility_state as state,
    facility_zip   as zip,

    order_created_ts,
    requested_ship_date,
    promised_delivery_date,
    actual_ship_ts,
    actual_delivery_date,

    carrier,
    tracking_number,
    priority_flag,

    total_amount_usd,
    currency,
    sales_region,

    {{ search_blob([
        'order_id',
        'purchase_order_id',
        'customer_name_norm',
        'facility_name_norm',
        'facility_name_alt',
        'facility_city',
        'facility_state',
        'facility_zip'
    ]) }} as search_blob

from {{ ref('fct_orders') }}
