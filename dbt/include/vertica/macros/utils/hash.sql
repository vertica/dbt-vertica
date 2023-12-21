{% macro vertica__hash(field) -%}
    md5(cast({{ field }} as varchar))
{%- endmacro %}