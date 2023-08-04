{% macro vertica__get_create_view_as_sql(relation, sql) -%}
  {{ return(create_view_as(relation, sql)) }}
{% endmacro %}


{% macro vertica__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set contract_config = config.get('contract') -%}
  {% if contract_config.enforced %}
     {{exceptions.warn("Model contracts cannot be enforced by <adapter>!")}}
  {% endif %}
  
  {{ sql_header if sql_header is not none }}
  create view {{ relation }} include schema privileges as (
    {{ sql }}
  );
{%- endmacro %}