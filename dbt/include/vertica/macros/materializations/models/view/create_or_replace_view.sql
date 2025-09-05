{% macro vertica__create_or_replace_view(relation, sql) %}
  {% set sql_header = config.get('sql_header', none) %}

  {{ sql_header if sql_header is not none }}
  create or replace view {{ relation }} as (
    {{ sql }}
  );
  {%- if 'dbadmin' != login_current %}
  alter view {{ relation }} owner to dbadmin;
  {%- endif -%}
{% endmacro %}
