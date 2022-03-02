{% macro vertica__alter_column_comment(relation, column_dict) -%}
  {{ exceptions.raise_not_implemented(
    'alter_column_comment macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{% macro vertica__alter_relation_comment(relation, relation_comment) -%}
  {{ exceptions.raise_not_implemented(
    'alter_relation_comment macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{# 
  No need to implement persist_docs(). Syntax supported by default. 
#}