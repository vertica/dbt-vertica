{% macro vertica__get_catalog(information_schema, schemas) -%}
  {% call statement('get_catalog', fetch_result=True) %}

    select
    '{{ information_schema.database }}' table_database
    , tab.table_schema
    , tab.table_name
    , 'TABLE' table_type
    , comment table_comment
    , tab.owner_name table_owner
    , col.column_name
    , col.ordinal_position column_index
    , col.data_type column_type
    , nullif('','') column_comment
    from v_catalog.tables tab
    join v_catalog.columns col on tab.table_id = col.table_id
    left join v_catalog.comments on tab.table_id = object_id
    where not(tab.is_system_table) and
        (
          {%- for schema in schemas -%}
            lower(tab.table_schema) = lower('{{ schema }}') {%- if not loop.last %} or {% endif %}
          {%- endfor -%}
        )
    union all
    select
    '{{ information_schema.database }}' table_database
    , vw.table_schema
    , vw.table_name
    , 'VIEW' table_type
    , comment table_comment
    , vw.owner_name table_owner
    , col.column_name
    , col.ordinal_position column_index
    , col.data_type column_type
    , nullif('','') column_comment
    from v_catalog.views vw
    join v_catalog.view_columns col on vw.table_id = col.table_id
    left join v_catalog.comments on vw.table_id = object_id
    where not(vw.is_system_view) and
        (
          {%- for schema in schemas -%}
            lower(vw.table_schema) = lower('{{ schema }}') {%- if not loop.last %} or {% endif %}
          {%- endfor -%}
        )
    order by table_schema, table_name, column_index

  {% endcall %}
  {{ return(load_result('get_catalog').table) }}
{% endmacro %}


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


{% macro vertica__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      table_name as name,
      table_schema as schema,
      'table' as type
    from v_catalog.tables
    where table_schema ilike '{{ schema_relation.schema }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      table_name as name,
      table_schema as schema,
      'view' as type
    from v_catalog.views
    where table_schema ilike '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}