{% macro vertica__get_merge_sql(target_relation, tmp_relation, dest_columns) %}
  {%- set dest_columns_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {%- set unique_key = config.get("unique_key", default = dest_columns | map(attribute="name")) -%}
  {%- set merge_update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="name") | list) -%}

  merge into {{ target_relation }} as DBT_INTERNAL_DEST
  using {{ tmp_relation }} as DBT_INTERNAL_SOURCE

  on HASH( {{ get_qouted_csv_with_prefix("DBT_INTERNAL_DEST", unique_key) }} ) = HASH({{ get_qouted_csv_with_prefix("DBT_INTERNAL_SOURCE", unique_key) }})

  when matched then update set
  {% for column_name in merge_update_columns -%}
    {{ adapter.quote(column_name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column_name) }}
    {%- if not loop.last %}, {% endif %}
  {%- endfor %}

  when not matched then insert
    ({{ dest_columns_csv }})
  values
  (
    {% for column in dest_columns -%}
       DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}
       {%- if not loop.last %}, {% endif %}
    {%- endfor %}
  )
{%- endmacro %}


{# No need to implement get_delete_insert_merge_sql(). Syntax supported by default. #}


{% macro vertica__get_insert_overwrite_merge_sql() -%}
  {{ exceptions.raise_not_implemented(
    'get_insert_overwrite_merge_sql macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}