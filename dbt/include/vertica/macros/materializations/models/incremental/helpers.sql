{% macro vertica__validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy") -%}
  {% if strategy == none %}
    {% set strategy = "append" %}
  {% endif %}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'delete+insert','insert_overwrite','append'
  {%- endset %}
  {% if strategy not in ['merge', 'delete+insert','insert_overwrite','append'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro get_qouted_csv_with_prefix(prefix, column_names) %}
  {% set quoted = [] %}

  {% for col in column_names -%}
      {%- do quoted.append(prefix + "." + adapter.quote(col)) -%}
  {%- endfor %}

  {%- set dest_cols_csv = quoted | join(', ') -%}

  {{ return(dest_cols_csv) }}
{% endmacro %}

{% macro vertica__get_incremental_sql(strategy, target_relation, tmp_relation, unique_key, dest_columns) %}
  {% if strategy == 'merge' %}
    {% do return(vertica__get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'delete+insert' %}
    {% do return(vertica__get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'insert_overwrite' %}
    {% do return(vertica__get_insert_overwrite_merge_sql(target_relation, tmp_relation, dest_columns)) %}
  {% elif strategy == 'append' %}
    {% do return(vertica__get_incremental_append_sql(target_relation, tmp_relation,  dest_columns)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}

{% macro vertica__get_table_in_relation(relation) -%}
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
        where table_schema = '{{ relation.schema}}'
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
  {{ return(load_result('get_columns_in_relation').table) }}
{% endmacro %}