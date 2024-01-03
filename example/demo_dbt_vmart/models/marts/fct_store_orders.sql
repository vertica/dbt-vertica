/* 
    Create table fct_store_orders and partition data by order date year
    Vertica creates a partition key for each unique date_ordered year. 
*/
{{ config(
    materialized = 'table',
    partition_by_string = 'YEAR(date_ordered)'
    ) 
}}

with store_orders as (
    select * from {{ ref('stg_store_orders') }}
),

fact as (
    select
        product_key,
        product_version,
        store_key,
        vendor_key,
        employee_key,
        order_number,
        date_ordered,
        date_shipped,
        expected_delivery_date,
        date_delivered,
        (date_delivered - date_ordered) as days_to_deliver,
        quantity_ordered,
        quantity_delivered,
        shipper_name,
        unit_price,
        shipping_cost,
        ((quantity_delivered * unit_price) + shipping_cost) as total_order_cost,
        quantity_in_stock,
        reorder_level,
        overstock_ceiling,
        loaded_at_timestamp,
        1 as order_count,
        case when (quantity_delivered = quantity_ordered) 
            then 1 else 0 end as quantity_accuracy_flag,
        case when (date_delivered <= expected_delivery_date) 
            then 1 else 0 end as on_time_delivery_flag,
        case when (quantity_delivered = quantity_ordered) and 
            (date_delivered <= expected_delivery_date)
            then 1 else 0 end as perfect_order_flag
    from
        store_orders
),

final as (
    select * from fact
)

select * from final
 
    