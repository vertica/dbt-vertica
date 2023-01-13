{% macro vertica__get_create_view_as_sql(relation, sql) -%}
  {{ return(create_view_as(relation, sql)) }}
{% endmacro %}


{% macro vertica__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} include schema privileges as (
    {{ sql }}
  );
{%- endmacro %}