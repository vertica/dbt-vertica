# dbt-vertica Macro Implementations

Below is a table for what macros the current Vertica adapter supports for dbt. This is constantly improving and changing as both dbt adds new functionality, as well as the dbt-vertica driver improvies. This list is based upon dbt 1.0.3, with the list found under https://github.com/dbt-labs/dbt-core/tree/main/core/dbt/include/global_project/macros.

## dbt Macros - adapters

| dbt Macros - adapters     | Function                            | Implemented |
| ------------------------- | ----------------------------------- | ----------- |
| adapters/columns.sql      | get_columns_in_relation()           | Yes         |
| adapters/columns.sql      | sql_convert_columns_in_relation()   | No          |
| adapters/columns.sql      | get_columns_in_query()              | Yes         |
| adapters/columns.sql      | alter_column_type()                 | No          |
| adapters/columns.sql      | alter_relation_add_remove_columns() | No          |
| adapters/freshness.sql    | collect_freshness()                 | No          |
| adapters/freshness.sql    | current_timestamp()                 | Yes         |
| adapters/indexes.sql      | create_indexes()                    | Yes         |
| adapters/indexes.sql      | get_create_index_sql()              | No          |
| adapters/metadata.sql     | get_catalog()                       | Yes         |
| adapters/metadata.sql     | information_schema_name()           | Yes         |
| adapters/metadata.sql     | list_schemas()                      | Yes         |
| adapters/metadata.sql     | list_relations_without_caching()    | Yes         |
| adapters/persist_docs.sql | alter_column_comment()              | No          |
| adapters/persist_docs.sql | alter_relation_comment()            | No          |
| adapters/persist_docs.sql | persist_docs()                      | Yes         |
| adapters/relation.sql     | drop_relation()                     | Yes         |
| adapters/relation.sql     | drop_relation_if_exists()           | Yes         |
| adapters/relation.sql     | get_or_create_relation()            | No          |
| adapters/relation.sql     | load_relation()                     | No          |
| adapters/relation.sql     | make_temp_relation()                | Yes         |
| adapters/relation.sql     | rename_relation()                   | Yes         |
| adapters/relation.sql     | truncate_relation()                 | Yes         |
| adapters/schema.sql       | create_schema()                     | Yes         |
| adapters/schema.sql       | drop_schema()                       | Yes         |

## dbt Macros - materializations
| dbt Macros - materializations                            | Function                         | Implemented |
| -------------------------------------------------------- | -------------------------------- | ----------- |
| materializations/models/incremental/merge.sql            | get_merge_sql()                  | Yes         |
| materializations/models/incremental/merge.sql            | get_delete_insert_merge_sql()    | Yes         |
| materializations/models/incremental/merge.sql            | get_insert_overwrite_merge_sql() | No          |
| materializations/models/table/create_table_as.sql        | create_table_as()                | Yes         |
| materializations/models/view/create_or_replace_view.sql  | create_or_replace_view()         | Yes         |
| materializations/models/view/create_view_as.sql          | create_view_as()                 | Yes         |
| materializations/models/view/create_view_as.sql          | get_create_view_as_sql()         | Yes         |
| materializations/seeds/helpers.sql                       | create_csv_table()               | Yes         |
| materializations/seeds/helpers.sql                       | reset_csv_table()                | Yes         |
| materializations/seeds/helpers.sql                       | get_binding_char()               | Yes         |
| materializations/seeds/helpers.sql                       | get_seed_column_quoted_csv()     | Yes         |
| materializations/seeds/helpers.sql                       | load_csv_rows()                  | Yes         |
| materializations/snapshots/helper.sql                    | build_snapshot_table()           | Yes         |
| materializations/snapshots/helper.sql                    | create_columns()                 | Yes         |
| materializations/snapshots/helper.sql                    | post_snapshot()                  | Yes         |
| materializations/snapshots/helper.sql                    | snapshot_staging_table()         | Yes         |
| materializations/snapshots/snapshot_merge.sql            | snapshot_merge_sql()             | Yes         |
| materializations/snapshots/strategies.sql                | snapshot_get_time()              | Yes         |
| materializations/snapshots/strategies.sql                | snapshot_hash_arguments()        | Yes         |
| materializations/snapshots/strategies.sql                | snapshot_string_as_time()        | Yes         |
| materializations/configs.sql                             | set_sql_header()                 | Yes         |
| materializations/configs.sql                             | should_full_refresh()            | Yes         |
| materializations/configs.sql                             | should_store_failures()          | Yes         |
| materializations/hooks.sql                               | run_hooks()                      | Yes         |
| materializations/hooks.sql                               | make_hook_config()               | Yes         |
| materializations/hooks.sql                               | before_begin()                   | Yes         |
| materializations/hooks.sql                               | in_transaction()                 | Yes         |
| materializations/hooks.sql                               | after_commit()                   | Yes         |