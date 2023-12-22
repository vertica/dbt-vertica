


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



{% macro vertica__get_relation_last_modified(information_schema, relations) -%}

  {%- call statement('last_modified', fetch_result=True) -%}
        select table_schema as schema,
              table_name as identifier,
              create_time as last_modified,
               {{ current_timestamp() }} as snapshotted_at 
        from v_catalog.tables
        where (
          {%- for relation in relations -%}
            (upper(table_schema) = upper('{{ relation.schema }}') and
             upper(table_name) = upper('{{ relation.identifier }}')){%- if not loop.last %} or {% endif -%}
          {%- endfor -%}
        )
  {%- endcall -%}

  {{ return(load_result('last_modified')) }}

{% endmacro %}




