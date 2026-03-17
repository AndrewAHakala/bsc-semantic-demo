{{
    config(
        materialized='table',
        cluster_by=['order_created_ts::date', 'status']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

facilities as (
    select * from {{ ref('stg_facilities') }}
)

select
    o.order_id,
    o.purchase_order_id,
    o.customer_account_id,
    o.facility_id,
    o.contact_id,

    c.customer_name,
    c.customer_name_norm,
    c.account_type,

    f.facility_name,
    f.facility_name_norm,
    f.facility_name_alt,
    f.city         as facility_city,
    f.state        as facility_state,
    f.zip          as facility_zip,

    o.order_created_ts,
    o.requested_ship_date,
    o.promised_delivery_date,
    o.actual_ship_ts,
    o.actual_delivery_date,

    o.status,
    o.status_last_updated_ts,
    o.is_fulfilled,
    o.days_to_last_update,

    o.priority_flag,
    o.carrier,
    o.tracking_number,
    o.sales_region,
    o.total_amount_usd,
    o.currency

from orders o
inner join customers c on c.customer_account_id = o.customer_account_id
inner join facilities f on f.facility_id = o.facility_id
