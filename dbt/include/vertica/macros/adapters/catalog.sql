{% macro vertica__get_catalog(information_schema, schemas) -%}

   {% set query %}     

        with tables as (
              {{ vertica__get_catalog_tables_sql(information_schema) }}
            {{ vertica__get_catalog_schemas_where_clause_sql(schemas) }}
             ),
        column as (
             {{ vertica__get_catalog_columns_sql(information_schema) }}
             {{ vertica__get_catalog_schemas_where_clause_sql(schemas) }}
          
        )
        vertica__get_catalog_results_sql()

         {%- endset -%}
          {{ return(run_query(query)) }}

{%- endmacro %}

    
  
  


{% macro vertica__get_catalog_relations(information_schema, relations) -%}
   {% set query %}

           
            with tables as (
                {{ vertica__get_catalog_tables_sql(information_schema) }}
                {{ vertica__get_catalog_relations_where_clause_sql(relations) }}
             ),
        column as (
             {{ vertica__get_catalog_columns_sql(information_schema) }}
             {{ vertica__get_catalog_relations_where_clause_sql(relations) }}
          
        )
        vertica__get_catalog_results_sql()

         {%- endset -%}

          {{ return(run_query(query)) }}

{%- endmacro %}








{% macro vertica__get_catalog_tables_sql(information_schema) -%}
    
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


{%- endmacro %}
 


{% macro vertica__get_catalog_columns_sql(information_schema) -%}
    select
       '{{ information_schema.database }}' table_database
        , tab.table_schema as "table_schema"
        , tab.table_name
        , 'TABLE' table_type
        , comment table_comment
        , tab.owner_name table_owner
        , col.column_name
        , col.ordinal_position column_index
        , col.data_type column_type
        , nullif('','') column_comment
        from {{ information_schema }}.columns

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
{%- endmacro %}

{% macro vertica__get_catalog_results_sql() -%}
    select *
    from tables
    join columns using ("table_database", "table_schema", "table_name")
    order by "column_index"
{%- endmacro %}

{% macro vertica__get_catalog_schemas_where_clause_sql(schemas) -%}
        where not(tab.is_system_table) and
        (
          {%- for schema in schemas -%}
            lower(tab.table_schema) = lower('{{ schema }}') {%- if not loop.last %} or {% endif %}
          {%- endfor -%}
        )
         where not(vw.is_system_view) and


        (
          {%- for schema in schemas -%}
            lower(vw.table_schema) = lower('{{ schema }}') {%- if not loop.last %} or {% endif %}
          {%- endfor -%}
        )

         order by table_schema, table_name, column_index

{%- endmacro %}


{% macro vertica__get_catalog_relations_where_clause_sql(relations) -%}
    where (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    upper("table_schema") = upper('{{ relation.schema }}')
                    and upper("table_name") = upper('{{ relation.identifier }}')
                )
            {% elif relation.schema %}
                (
                    upper("table_schema") = upper('{{ relation.schema }}')
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}

            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}
