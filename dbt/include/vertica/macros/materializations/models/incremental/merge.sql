{% macro vertica__get_merge_sql(target_relation, tmp_relation, dest_columns) %}
  {%- set dest_columns_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {%- set unique_key = config.get("unique_key", default = dest_columns | map(attribute="name")) -%}
  {%- set merge_update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="name") | list) -%}
  {{vertica__create_table_as(True, tmp_relation, sql)}}
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


{% macro vertica__get_delete_insert_merge_sql(target, source, dest_columns) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set unique_key = config.get('unique_key', default = dest_columns | map(attribute="name") | list) -%}
    {%- set unique_key_columns_csv = get_quoted_csv(unique_key) -%}
    {{vertica__create_table_as(True, source, sql)}}
    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{target }}
            where (
                HASH( {{ unique_key_columns_csv }} ) in (
                select HASH({{  unique_key_columns_csv }})
                                from {{ source }} )
            );
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key}}) in (
                select {{ unique_key }}
                from {{ source }}
            );

        {% endif %}
        {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}

{% macro vertica__get_insert_overwrite_merge_sql(target, source, dest_columns) -%}
    {%- set partition_by = config.get('partition_by', default = dest_columns | map(attribute="name") | list) -%}
    {%- set partitions = config.get('partitions', default = dest_columns | map(attribute="name") | list) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {{vertica__create_table_as(True, source, sql)}}
    {% for partition in partitions -%}
       SELECT DROP_PARTITIONS('{{target.schema}}.{{target.table}}',{{partition}},{{partition}});
       SELECT PURGE_PARTITION('{{target.schema}}.{{target.table}}',{{partition}});
      {%- if not loop.last %} {% endif %}
    {%- endfor %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{% endmacro %}



