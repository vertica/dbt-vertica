{% macro vertica__safe_cast(field, type) %}
 cast({{field}} as {{type}})
{% endmacro %}