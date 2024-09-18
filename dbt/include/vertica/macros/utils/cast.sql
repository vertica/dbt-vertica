{% macro cast(field, type) %}
  {{ return(adapter.dispatch('cast', 'dbt') (field, type)) }}
{% endmacro %}

{% macro vertica__cast(field, type) %}
    cast({{field}} as {{type}})
{% endmacro %}