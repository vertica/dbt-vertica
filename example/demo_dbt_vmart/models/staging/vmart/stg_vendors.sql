with source as (
	select * from {{ source('raw_data', 'vendors') }}
),

staged as (
    select 
        vendorid as vendor_key,
        vendorname as vendor_name,
        vendoraddress as vendor_address,
        vendorcity as vendor_city,
        vendorstate as vendor_state, 
        lastdealupdate as last_deal_update,
        loadedattimestamp as loaded_at_timestamp
    from source
)

select * from staged
