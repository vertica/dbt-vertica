{% macro vertica__validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {#-- See: https://github.com/dbt-labs/dbt-core/blob/1071a4681df91633301fdf23e34de819b66fbeb7/core/dbt/include/global_project/macros/materializations/models/incremental/incremental.sql#L52 #}
  {% set strategy = config.get('incremental_strategy') or 'merge' %}

  {#-- Try to set a default instead of None, but this doesn't help the tests #}
  {% if strategy is None or strategy == 'None' %}
    {% set strategy = 'merge' %}
  {% endif %}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'delete+insert'
  {%- endset %}
  {% if strategy not in ['merge', 'delete+insert'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}


{% macro vertica__get_incremental_sql(strategy, tmp_relation, target_relation, dest_columns) %}
  {% if strategy == 'merge' %}
    {% do return(vertica__get_merge_sql(target_relation, tmp_relation, dest_columns)) %}
  {% elif strategy == 'delete+insert' %}
    {% do return(get_delete_insert_merge_sql(target_relation, tmp_relation, dest_columns)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}