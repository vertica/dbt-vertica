{% macro vertica__get_view_current_owner(relation) %}
    {{ log("relation is " ~relation, info=False) }}
    {% set table_schema = relation.schema %}
    {% set table_name = relation.identifier %}
    {{ log("table_schema is " ~table_schema, info=False) }}
    {{ log("table_name is " ~table_name, info=False) }}

    {{ log("username is " ~target.user, info=False) }}

    {%- set query -%}
        SELECT owner_name
        FROM v_catalog.views
        WHERE table_schema = '{{table_schema}}'
          AND table_name = '{{table_name}}'
    {%- endset -%}

    {%- set results = run_query(query) -%}

    {{ log("result is " ~results, info=False) }}
    {%- if results and results.rows -%}
        {%- set owner = results.rows[0][0] -%}
        {{ return(owner) }}
        {{ log("macros get_view_current_owner():" ~ table_schema~'.'~table_name~ " owner:" ~owner, info=True) }}
    {%- endif -%}
{% endmacro %}
