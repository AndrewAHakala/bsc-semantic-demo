with source as (
    select * from {{ source('demo_bsc_raw', 'ORDER_FACT') }}
),

cleaned as (
    select
        order_id,
        purchase_order_id,
        customer_account_id,
        facility_id,
        contact_id,

        order_created_ts,
        requested_ship_date,
        promised_delivery_date,
        actual_ship_ts,
        actual_delivery_date,

        status,
        status_last_updated_ts,

        priority_flag,
        carrier,
        tracking_number,
        sales_region,
        total_amount_usd,
        currency,
        created_at,

        case
            when status in ('SHIPPED', 'DELIVERED') then true
            else false
        end as is_fulfilled,

        datediff('day', order_created_ts, status_last_updated_ts) as days_to_last_update

    from source
)

select * from cleaned
