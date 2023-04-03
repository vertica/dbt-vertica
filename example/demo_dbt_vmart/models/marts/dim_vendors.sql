{{ config (
    materialized = 'table'
    )
}}

with vendors as (
    select * from {{ ref('stg_vendors') }}
),

states as (
    select * from {{ ref('stg_states') }}
),

joined as (
    select
        vendor_key,
        vendor_name,
        vendor_address,
        vendor_city,
        vendor_state, 
        states.region as vendor_region, 
        last_deal_update,
        loaded_at_timestamp
    from
        vendors inner join states on
        vendors.vendor_state = states.state_code
),

final as (
    select * from joined
)

select * from final