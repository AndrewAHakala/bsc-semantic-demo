with source as (
    select * from {{ source('demo_bsc_raw', 'CUSTOMER_DIM') }}
),

cleaned as (
    select
        customer_account_id,
        customer_name,
        {{ normalize_text('customer_name') }} as customer_name_norm,
        account_type,
        territory,
        sales_region,
        created_at
    from source
)

select * from cleaned
