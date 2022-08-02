{% macro vertica__get_merge_sql(target_relation, tmp_relation, dest_columns) %}
  {%- set complex_type = config.get('include_complex_type', default = False) -%}
  {%- set dest_columns_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {%- set unique_key = config.get("unique_key", default = dest_columns | map(attribute="name")) -%}
  {%- set merge_update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="name")) -%}

  {%- if complex_type %}
      {{vertica__create_table_from_relation(True, tmp_relation, target_relation, dest_columns, sql)}}
  {% else %}
      {{vertica__create_table_as(True, tmp_relation, sql)}}
  {% endif %}

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


{% macro vertica__get_delete_insert_merge_sql(target_relation, tmp_relation, dest_columns) -%}
    {%- set complex_type = config.get('include_complex_type') -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set unique_key = config.get('unique_key', default = dest_columns | map(attribute="name")) -%}
    {%- set unique_key_columns_csv = get_quoted_csv(unique_key) -%}

    {%- if complex_type %}
        {{ vertica__create_table_from_relation(True, tmp_relation, target_relation, dest_columns, sql) }}
    {% else %}
        {{ vertica__create_table_as(True, tmp_relation, sql) }}
    {% endif %}


    {% if unique_key is sequence and unique_key is not string %}
        delete from {{ target_relation }}
        where (
            HASH( {{ unique_key_columns_csv }} ) in (
            select HASH({{  unique_key_columns_csv }})
                            from {{ tmp_relation }} )
        );
    {% else %}
        delete from {{ target_relation }}
        where (
            {{ unique_key }}) in (
            select {{ unique_key }}
            from {{ tmp_relation }}
        );

    {% endif %}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ tmp_relation }}
    )

{%- endmacro %}

{% macro vertica__get_insert_overwrite_merge_sql(target_relation, tmp_relation, dest_columns) -%}
    {%- set complex_type = config.get('include_complex_type') -%}
    {%- set partitions = config.get('partitions') -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {%- if complex_type %}
        {{ vertica__create_table_from_relation(True, tmp_relation, target_relation, dest_columns, sql) }}
    {% else %}
        {{ vertica__create_table_as(True, tmp_relation, sql) }}
    {% endif %}


    {% for partition in partitions %}
    SELECT DROP_PARTITIONS('{{ target_relation.schema }}.{{ target_relation.table }}', '{{ partition }}', '{{ partition }}');
    SELECT PURGE_PARTITION('{{ target_relation.schema }}.{{ target_relation.table }}', '{{ partition }}');
    {% endfor %}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ tmp_relation }}
    )
{% endmacro %}



