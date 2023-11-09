
          


DROP TABLE IF EXISTS  "VMart"."my_schema"."stg_store_orders_insert_overwrite";
    create table
        "VMart"."my_schema"."stg_store_orders_insert_overwrite" as select * from
      "VMart"."public"."stg_store_orders_insert_overwrite"

      

      