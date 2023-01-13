{% macro vertica__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% do return(base_relation.incorporate(
                                  path={
                                    "identifier": tmp_identifier,
                                    "schema": none,
                                    "database": none
                                  })) -%}
{% endmacro %}


{% macro vertica__rename_relation(from_relation, to_relation) %}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter {{ from_relation.type }} {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}


{% macro vertica__load_relation(relation) -%}
  {{ exceptions.raise_not_implemented(
    'load_relation macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{#
  No need to implement drop_relation(). Syntax supported by default. 
  No need to implement drop_relation_if_exists(). Syntax supported by default.
  No need to implement get_or_create_relation(). Syntax supported by default.
#}