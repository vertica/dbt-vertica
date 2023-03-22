{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
    full_refresh= true,
    merge_update_columns = ["col1", "load_val"] 
        )
}}

select *
from {{ source('raw_data', 'test_s3') }}  



