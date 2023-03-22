with source as (
	select * from {{ source('raw_data', 'states') }}
),

staged as (
    select
        statename as state_name,
        statecode as state_code,
        region,
        division
    from source
)

select * from staged