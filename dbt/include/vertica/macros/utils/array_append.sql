{# new_element must be the same data type as elements in array to match postgres functionality #}
{% macro vertica__array_append(array, new_element) -%}
    {{ array_concat(array, array_construct([new_element])) }}
{%- endmacro %}
