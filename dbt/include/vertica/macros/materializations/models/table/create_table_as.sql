{% macro vertica__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set order_by = config.get('order_by', default=none) -%}
  {%- set segmented_by_string = config.get('segmented_by_string', default=none) -%}
  {%- set segmented_by_all_nodes = config.get('segmented_by_all_nodes', default=True) -%}
  {%- set partition_by_string = config.get('partition_by_string', default=none) -%}
  {%- set partition_by_group_by_string = config.get('partition_by_group_by_string', default=none) -%}
  {%- set partition_by_active_count = config.get('partition_by_active_count', default=none) -%}
  {%- set ksafe = config.get('ksafe', default=none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}local temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {% if temporary: -%}on commit preserve rows{%- endif %}
  as (
    {{ sql }}
  )
  {% if order_by is not none -%}
  order by {{ order_by | join(',') }}
  {% endif %}
  {% if segmented_by_string is not none -%}
  segmented by {{ segmented_by_string }} {% if segmented_by_all_nodes %} ALL NODES {% endif %}
  {% if ksafe is not none -%}
  all nodes ksafe{% if database is not none and "docker" in database %} 0 {% else %} {{ ksafe }} {% endif %}
  {% endif %}
  {% if partition_by_string is not none -%}
  partition by {{ partition_by_string }}
  {% if partition_by_group_by_string is not none -%}
  group by {{ partition_by_group_by_string }}
  {% endif %}
  {% if partition_by_active_count is not none -%}
  ACTIVEPARTITIONCOUNT {{ partition_by_active_count }}
  {% endif %}
  {% endif %}
  ;
{% endmacro %}