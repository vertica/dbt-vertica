

{% macro vertica__can_clone_table() %}
    {{ return(adapter.dispatch('can_clone_table', 'dbt')()) }}
{% endmacro %}



