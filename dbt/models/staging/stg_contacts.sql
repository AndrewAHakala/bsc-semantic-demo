with source as (
    select * from {{ source('demo_bsc_raw', 'CONTACT_DIM') }}
),

cleaned as (
    select
        contact_id,
        customer_account_id,
        facility_id,
        contact_name,
        {{ normalize_text('contact_name') }} as contact_name_norm,
        contact_role,
        email,
        created_at
    from source
)

select * from cleaned
