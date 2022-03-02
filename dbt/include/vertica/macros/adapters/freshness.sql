{% macro vertica__current_timestamp() -%}
  current_timestamp
{%- endmacro %}


{% macro vertica__collect_freshness() -%}
  {{ exceptions.raise_not_implemented(
    'collect_freshness macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}