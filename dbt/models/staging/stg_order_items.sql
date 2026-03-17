with source as (
    select * from {{ source('demo_bsc_raw', 'ORDER_ITEM_FACT') }}
),

cleaned as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price_usd,
        line_total_usd,
        created_at
    from source
)

select * from cleaned
