{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_number',
    merge_update_columns = ['date_delivered', 'quantity_delivered', 'shipping_cost'] 
    )
}}

with new_store_orders as (
	select * from {{ source('raw_data', 'store_orders') }}
    {% if is_incremental() %}
        where loadedattimestamp >= ( select max(loaded_at_timestamp) from {{this}} )
    {% endif %}
),

updated_store_orders as (
    select 
        productid as product_key,
        productversion as product_version,
        storeid as store_key,
        vendorid as vendor_key,
        employeeid as employee_key,
        ordernumber as order_number,
        dateordered as date_ordered,
        dateshipped as date_shipped,
        expecteddeliverydate as expected_delivery_date,
        datedelivered as date_delivered,
        qtyordered as quantity_ordered,
        qtydelivered as quantity_delivered,
        shippername as shipper_name,
        unitprice as unit_price,
        shippingcost as shipping_cost,
        qtyinstock as quantity_in_stock,
        reorderlevel as reorder_level,
        overstockceiling as overstock_ceiling,
        loadedattimestamp as loaded_at_timestamp
    from new_store_orders
)

select * from updated_store_orders