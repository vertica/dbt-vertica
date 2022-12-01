{% macro vertica__validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {#-- See: https://github.com/dbt-labs/dbt-core/blob/1071a4681df91633301fdf23e34de819b66fbeb7/core/dbt/include/global_project/macros/materializations/models/incremental/incremental.sql#L52 #}
  {% set strategy = config.get('incremental_strategy') or 'merge' %}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'delete+insert'
  {%- endset %}
  {% if strategy not in ['merge', 'delete+insert'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro vertica__get_incremental_sql(strategy, target_relation, tmp_relation, unique_key, dest_columns) %}
  {% if strategy == 'merge' %}
    {% do return(vertica__get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'delete+insert' %}
    {% do return(vertica__get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}