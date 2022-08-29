{% macro vertica__get_merge_sql(target_relation, dest_columns, sql) %}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set dest_columns_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {%- set unique_key = config.get("unique_key", default = dest_columns | map(attribute="name")) -%}
  {%- set merge_update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="name")) -%}

  {{ sql_header if sql_header is not none }}

  merge into {{ target_relation }} as DBT_INTERNAL_DEST
  using (
            {{ sql }}
        ) as DBT_INTERNAL_SOURCE

  on HASH( {{ get_qouted_csv_with_prefix("DBT_INTERNAL_DEST", unique_key) }} ) = HASH({{ get_qouted_csv_with_prefix("DBT_INTERNAL_SOURCE", unique_key) }})

  {%- if merge_update_columns is string %}
    {%- set merge_update_columns = merge_update_columns.split(',') %}
  {% endif %}

  {%- if merge_update_columns | length > 0 %}
    when matched then update set
    {% for column_name in merge_update_columns -%}
      {{ adapter.quote(column_name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column_name) }}
      {%- if not loop.last %}, {% endif %}
    {%- endfor %}
  {%- endif %}

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


{% macro vertica__get_delete_insert_merge_sql(target_relation, dest_columns, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set condition_columns = config.get('condition_columns', default = dest_columns | map(attribute="name")) -%}
    {%- set condition_columns_csv = get_quoted_csv(condition_columns) -%}

    {{ sql_header if sql_header is not none }}


    {% if condition_columns is sequence and condition_columns is not string %}
        delete from {{ target_relation }}
        where (
            HASH( {{ condition_columns_csv }} ) in (
            select HASH({{ condition_columns_csv }})
            from (
                     {{ sql }}
                 ) as DBT_MASKED_TARGET )
        );
    {% else %}
        delete from {{ target_relation }}
        where (
            {{ condition_columns }}) in (
            select {{ condition_columns }}
            from (
                     {{ sql }}
                 ) as DBT_MASKED_TARGET
        );

    {% endif %}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from (
                 {{ sql }}
             ) as DBT_MASKED_TARGET
    )
{%- endmacro %}


{% macro vertica__get_insert_overwrite_merge_sql(target_relation, dest_columns, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set partitions = config.get('partitions') -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {{ sql_header if sql_header is not none }}

    {% for partition in partitions %}
    SELECT DROP_PARTITIONS('{{ target_relation.schema }}.{{ target_relation.table }}', '{{ partition }}', '{{ partition }}');
    SELECT PURGE_PARTITION('{{ target_relation.schema }}.{{ target_relation.table }}', '{{ partition }}');
    {% endfor %}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from (
                 {{ sql }}
             ) as DBT_MASKED_TARGET
    )
{% endmacro %}



