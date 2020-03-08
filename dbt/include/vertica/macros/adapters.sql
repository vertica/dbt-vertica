
{% macro vertica__information_schema_name(database) -%}
  {%- if database -%}
    {{ adapter.quote_as_configured(database, 'database') }}.v_catalog
  {%- else -%}
    v_catalog
  {%- endif -%}
{%- endmacro %}

{% macro vertica__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select schema_name
    from v_catalog.schemata
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro vertica__check_schema_exists(database, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
        select count(*)
        from v_catalog.schemata
        where schema_name='{{ schema }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro vertica__drop_schema(database_name, schema_name) -%}
  {% call statement('drop_schema') -%}
    drop schema {{database_name}}.{{schema_name}} cascade
  {% endcall %}
{% endmacro %}

{% macro vertica__create_schema(database_name, schema_name) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{database_name}}.{{schema_name}}
  {% endcall %}
{% endmacro %}

{% macro vertica__list_relations_without_caching(information_schema, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ information_schema.database }}' as database,
      table_name as name,
      table_schema as schema,
      'table' as type
    from v_catalog.tables
    where table_schema ilike '{{ schema }}'
    union all
    select
      '{{ information_schema.database }}' as database,
      table_name as name,
      table_schema as schema,
      'view' as type
    from v_catalog.views
    where table_schema ilike '{{ schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
  {% endmacro %}

{% macro vertica__rename_relation(from_relation, to_relation) %}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter {{ from_relation.type }} {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}

{# macro vertica__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }} cascade
  {%- endcall %}
{% endmacro #}

{# macro vertica__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro #}


{# macro vertica__alter_column_type(relation, column_name, new_column_type) -#}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {#
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} add column {{ adapter.quote(tmp_column) }} {{ new_column_type }};
    update {{ relation }} set {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
    alter table {{ relation }} drop column {{ adapter.quote(column_name) }} cascade;
    alter table {{ relation }} rename column {{ adapter.quote(tmp_column) }} to {{ adapter.quote(column_name) }}
  {% endcall %}

{% endmacro %}
#}

{% macro vertica__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select 
    column_name
    , data_type
    , character_maximum_length 
    , numeric_precision 
    , numeric_scale 
    from v_catalog.columns
    where table_schema = '{{ relation.schema }}'
    and table_name = '{{ relation.identifier }}'
    order by ordinal_position 
  {% endcall %}
{% endmacro %}

{% macro vertica__create_view_as(relation, sql) %}
  {% set sql_header = config.get('sql_header', none) %}

  {{ sql_header if sql_header is not none }}
  create or replace view {{ relation }} as (
    {{ sql }}
  );

{% endmacro %}

{% macro vertica__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  as (
    {{ sql }}
  );
{% endmacro %}
