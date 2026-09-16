{#
  Vertica SQL (scalar) user-defined functions.

  Vertica SQL functions have the form:
      CREATE OR REPLACE FUNCTION schema.name(arg type, ...)
      RETURN return_type
      AS BEGIN
          RETURN expression;
      END;

  The function body must be a single RETURN expression (no FROM clauses or
  subqueries), so the leading SELECT of the dbt function model is stripped.
  Vertica does not accept a volatility specifier on SQL functions; if one is
  configured it is ignored with a warning.
#}

{% macro vertica__scalar_function_sql(target_relation) %}
    {%- set volatility = model.config.get('volatility') -%}
    {%- if volatility is not none -%}
        {%- do exceptions.warn("Vertica SQL functions do not support a volatility specifier; '" ~ volatility ~ "' on function '" ~ model.name ~ "' will be ignored") -%}
    {%- endif -%}
    {%- set body = model.compiled_code | trim -%}
    {%- if body[:6] | lower == 'select' -%}
        {%- set body = body[6:] | trim -%}
    {%- endif -%}
    {%- if body[-1:] == ';' -%}
        {%- set body = body[:-1] | trim -%}
    {%- endif %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql() }})
    RETURN {{ model.returns.data_type }}
    AS
    BEGIN
        RETURN ({{ body }});
    END;
{% endmacro %}


{% macro vertica__scalar_function_python(target_relation) %}
    {% do exceptions.raise_compiler_error("Python UDFs must be installed as compiled UDx libraries in Vertica (CREATE LIBRARY) and cannot be created through dbt function models. Only language 'sql' is supported by dbt-vertica.") %}
{% endmacro %}


{% macro vertica__scalar_function_javascript(target_relation) %}
    {% do exceptions.raise_compiler_error("JavaScript UDFs are not supported by Vertica. Only language 'sql' is supported by dbt-vertica.") %}
{% endmacro %}


{% macro vertica__aggregate_function_sql(target_relation) %}
    {% do exceptions.raise_compiler_error("Aggregate UDFs (UDAFs) must be installed as compiled UDx libraries in Vertica (CREATE LIBRARY) and cannot be created through dbt function models. Only scalar SQL functions are supported by dbt-vertica.") %}
{% endmacro %}


{% macro vertica__aggregate_function_python(target_relation) %}
    {% do exceptions.raise_compiler_error("Aggregate UDFs (UDAFs) must be installed as compiled UDx libraries in Vertica (CREATE LIBRARY) and cannot be created through dbt function models. Only scalar SQL functions are supported by dbt-vertica.") %}
{% endmacro %}
