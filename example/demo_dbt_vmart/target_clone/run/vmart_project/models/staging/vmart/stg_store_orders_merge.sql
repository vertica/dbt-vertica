
          


DROP TABLE IF EXISTS  "VMart"."my_schema"."stg_store_orders_merge";
    create table
        "VMart"."my_schema"."stg_store_orders_merge" as select * from
      "VMart"."public"."stg_store_orders_merge"

      

      