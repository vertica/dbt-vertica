{% macro vertica__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
    column_name
    , data_type
    , character_maximum_length
    , numeric_precision
    , numeric_scale
    from (
        select
        column_name
        , data_type
        , character_maximum_length
        , numeric_precision
        , numeric_scale
        , ordinal_position
        from v_catalog.columns
        where table_schema = '{{ relation.schema }}'
        and table_name = '{{ relation.identifier }}'
        union all
        select
        column_name
        , data_type
        , character_maximum_length
        , numeric_precision
        , numeric_scale
        , ordinal_position
        from v_catalog.view_columns
        where table_schema = '{{ relation.schema }}'
        and table_name = '{{ relation.identifier }}'
    ) t
    order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro vertica__sql_convert_columns_in_relation(table) -%}
  {{ exceptions.raise_not_implemented(
    'sql_convert_columns_in_relation macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{% macro vertica__alter_column_type(relation, column_name, new_column_type) -%}
  {{ exceptions.raise_not_implemented(
    'alter_column_type macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{% macro vertica__alter_relation_add_remove_columns(relation, column_name, new_column_type) -%}
  {{ exceptions.raise_not_implemented(
    'alter_relation_add_remove_columns macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{# 
  No need to implement get_columns_in_query(). Syntax supported by default. 
#}