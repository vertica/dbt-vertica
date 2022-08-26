{% macro vertica__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}local temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {% if temporary: -%}on commit preserve rows{%- endif %}
  as (
    {{ sql }}
  );
{% endmacro %}

{% macro vertica__create_table_from_relation(temporary, relation, target, dest_columns, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set ddl = vertica__get_ddl_in_relation(target) -%}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}local temporary{%- endif %} table {{ relation }}
     {% for row in ddl %}
       {%- set to_replace = "CREATE TABLE " + target.schema + "." + target.identifier -%}
       {%- set table_schema = (row.ddl | replace(to_replace, "")) | replace(";", "") | replace("PARTITION", "--PARTITION") -%}
       {{ table_schema }}
     {% endfor %} {% if temporary: -%}on commit preserve rows{%- endif %};

      insert into {{ relation }} ({{ dest_cols_csv }})
      (
        select {{ dest_cols_csv }} from (
         {{ sql }}
        ) as DBT_MASKED_TARGET
      );

{% endmacro %}

