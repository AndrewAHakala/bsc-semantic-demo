with source as (
    select * from {{ source('demo_bsc_raw', 'FACILITY_DIM') }}
),

cleaned as (
    select
        facility_id,
        customer_account_id,
        facility_name,
        {{ normalize_text('facility_name') }} as facility_name_norm,
        facility_name_alt,
        address_line1,
        city,
        state,
        zip,
        country,
        created_at
    from source
)

select * from cleaned
