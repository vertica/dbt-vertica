{% macro vertica__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set order_by_string = config.get('order_by_string', default=none) -%}
  {%- set segmented_by_string = config.get('segmented_by_string', default=none) -%}
  {%- set partition_by_string = config.get('partition_by_string', default=none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}local temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {% if temporary: -%}on commit preserve rows{%- endif %}
  as (
    {{ sql }}
  )
  {% if order_by_string is not none and not temporary -%}
  order by {{ order_by_string }}
  {% endif %}
  {% if segmented_by_string is not none and order_by_string is not none and not temporary -%}
  segmented by {{ segmented_by_string }}
  all nodes ksafe{% if database is not none and "docker" in database %} 0 {% else %} 1 {% endif %}
  {% endif %}
  {% if partition_by_string is not none and segmented_by_string is not none and not temporary -%}
  partition by {{ partition_by_string }}
  group by {{ partition_by_string }}
  {% endif %}
  ;
{% endmacro %}