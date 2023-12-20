{% macro array_construct(inputs, data_type) -%}
    {% if inputs|length > 0 %}
    ARRAY[ {{ inputs|join(' , ') }} ]
    {% else %}
    ARRAY[{{data_type}}]
    {% endif %}
{%- endmacro %}