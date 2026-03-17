with source as (
    select * from {{ source('demo_bsc_raw', 'PRODUCT_DIM') }}
),

cleaned as (
    select
        product_id,
        product_name,
        product_family,
        product_line,
        unit_cost_usd,
        created_at
    from source
)

select * from cleaned
