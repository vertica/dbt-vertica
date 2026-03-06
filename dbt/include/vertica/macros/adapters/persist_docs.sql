{#
  Code extracted from https://github.com/dbt-labs/dbt-adapters/blob/61801e9aa22c6bf219398b7fee9eefca96a85e7a/dbt-postgres/src/dbt/include/postgres/macros/adapters.sql#L197
#}


{#
  By using dollar-quoting like this, users can embed anything they want into their comments
  (including nested dollar-quoting), as long as they do not use this exact dollar-quoting
  label. It would be nice to just pick a new one but eventually you do have to give up.
#}
{% macro vertica_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  {%- set magic = '$dbt_comment_literal_block$' -%}
  {%- if magic in comment -%}
    {%- do exceptions.raise_compiler_error('The string ' ~ magic ~ ' is not allowed in comments.') -%}
  {%- endif -%}
  {{ magic }}{{ comment }}{{ magic }}
{%- endmacro %}



{% macro vertica__alter_column_comment(relation, column_dict) %}
  {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
  {% for column_name in column_dict if (column_name in existing_columns) %}
    {% set comment = column_dict[column_name]['description'] %}
    {% set escaped_comment = vertica_escape_comment(comment) %}
    comment on column {{ relation }}.{{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }} is {{ escaped_comment }};
  {% endfor %}
{%- endmacro %}


{% macro vertica__alter_relation_comment(relation, comment) %}
  {% set escaped_comment = vertica_escape_comment(comment) %}
  {% if relation.type == 'materialized_view' -%}
    {% set relation_type = "materialized view" %}
  {%- else -%}
    {%- set relation_type = relation.type -%}
  {%- endif -%}
  comment on {{ relation_type }} {{ relation }} is {{ escaped_comment }};
{% endmacro %}

{# 
  No need to implement persist_docs(). Syntax supported by default. 
#}
