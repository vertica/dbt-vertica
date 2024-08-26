{% macro vertica__safe_cast(field, type) %}
 {% if type|upper == "GEOMETRY" -%}
        try_to_geometry({{field}})
    {% elif type|upper == "GEOGRAPHY" -%}
        try_to_geography({{field}})
    {% else -%}
        {{ adapter.dispatch('cast', 'dbt')(field, type) }}
    {% endif -%}
{% endmacro %}
