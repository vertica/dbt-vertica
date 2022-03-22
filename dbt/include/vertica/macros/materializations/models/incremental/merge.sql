{% macro vertica__get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) %}
  {%- set dest_columns_csv =  get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {%- set merge_columns = config.get("merge_columns", default=None)%}

  merge into {{ target_relation }} as DBT_INTERNAL_DEST
  using {{ tmp_relation }} as DBT_INTERNAL_SOURCE

  {#-- Test 1, find the provided merge columns #}
  {% if merge_columns %}
    on 
    {% for column in merge_columns %}
      DBT_INTERNAL_DEST.{{ adapter.quote(column) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column) }}
      {%- if not loop.last %} AND {% endif %} 
    {%- endfor %}
  {#-- Test 2, use all columns in the destination table #}
  {% else %}
    on
    {% for column in dest_columns -%}
      DBT_INTERNAL_DEST.{{ adapter.quote(column.name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }} 
      {%- if not loop.last %} AND {% endif %}
    {%- endfor %}
  {% endif %}

  when matched then update set
  {% for column in dest_columns -%}
    {{ adapter.quote(column.name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}
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


{% macro vertica__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            );

    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    );

{%- endmacro %}


{% macro vertica__get_insert_overwrite_merge_sql() -%}
  {{ exceptions.raise_not_implemented(
    'get_insert_overwrite_merge_sql macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}