/*
    Table and projection data are segmented by hash on the column vendor_key 
    and sorted on the same column on which it is segmented.
    Data is evenly distributed across all cluster nodes. 
*/
{{ config(
    materialized = 'table',
    order_by = 'vendor_key',
    segmented_by_string = 'vendor_key'
    ) 
}}

with vendors as (
    select * from {{ ref('dim_vendors') }}
),

store_orders as (
    select * from {{ ref('fct_store_orders') }}
),

joined as (
    select
        store_orders.vendor_key as "vendor_key",
        vendors.vendor_name as "Vendor Name",
        vendors.vendor_state as "Vendor State",
        vendors.vendor_region as "Vendor Region",
        avg(quantity_ordered) as "Avg Quatity Ordered",
        avg(quantity_delivered) as "Avg Quantity Delivered",
        avg(days_to_deliver) as "Avg Days to Deliver",
        avg(shipping_cost) as "Avg Shipping Cost",    
        avg(total_order_cost) as "Avg Order Cost",
        sum(quantity_accuracy_flag) / count(order_count) as "Quantity Accuracy Rate",
        sum(on_time_delivery_flag) / count(order_count) as "On-time Delivery Rate",
        sum(perfect_order_flag) / count(order_count) as "Perfect Order Rate"
    from
        store_orders inner join vendors on
        vendors.vendor_key = store_orders.vendor_key
    group by
        1, 2, 3, 4
),

final as (
    select * from joined
)

select * from final