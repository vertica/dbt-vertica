{% macro vertica__cast_bool_to_text(field) %}
  case when {{ field }} then 'true' when {{ field }} is null then null else 'false' end
{% endmacro %}
