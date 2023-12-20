{% macro array_concat(array_1, array_2) -%}
    array_cat({{ array_1 }}, {{ array_2 }})
{%- endmacro %}