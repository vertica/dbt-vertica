{% macro vertica__get_create_index_sql(relation, index_dict) -%}
  {{ exceptions.raise_not_implemented(
    'get_create_index_sql macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{# 
  No need to implement create_indexes(). Syntax supported by default. 
#}