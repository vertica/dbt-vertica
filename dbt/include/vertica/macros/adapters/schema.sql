{% macro vertica__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier().include(database=False) }}
  {% endcall %}
{% endmacro %}


{% macro vertica__drop_schema(relation) -%}
  {% call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier().include(database=False) }} cascade
  {% endcall %}
{% endmacro %}