{% macro vertica__get_columns_in_relation(relation) -%}
 
   {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
    column_name
    , data_type
    , character_maximum_length
    , numeric_precision
    , numeric_scale
    from (
        select
        column_name
        , data_type
        , character_maximum_length
        , numeric_precision
        , numeric_scale
        , ordinal_position
        from v_catalog.columns
        where table_schema = '{{ relation.schema }}'
        and table_name = '{{ relation.identifier }}'
        union all
        select
        column_name
        , data_type
        , character_maximum_length
        , numeric_precision
        , numeric_scale
        , ordinal_position
        from v_catalog.view_columns
        where table_schema = '{{ relation.schema }}'
        and table_name = '{{ relation.identifier }}'
    ) t
    order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}

  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro vertica__sql_convert_columns_in_relation(table) -%}
  {{ exceptions.raise_not_implemented(
    'sql_convert_columns_in_relation macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}



{% macro vertica__get_columns_in_temp_relation(relation) -%}
 
   {% call statement('get_columns_in_temp_relation', fetch_result=True) %}
    select
    column_name
    , data_type
    , character_maximum_length
    , numeric_precision
    , numeric_scale
    from (
        select
        column_name
        , data_type
        , character_maximum_length
        , numeric_precision
        , numeric_scale
        , ordinal_position
        from v_catalog.columns
        where  table_name = '{{ relation.identifier }}'
        union all
        select
        column_name
        , data_type
        , character_maximum_length
        , numeric_precision
        , numeric_scale
        , ordinal_position
        from v_catalog.view_columns
        where table_name = '{{ relation.identifier }}'
    ) t
    order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_temp_relation').table %}

  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}







{% macro sql_convert_columns_in_relation(table) -%}
  {% set columns = [] %}
  {% for row in table %}
    {% do columns.append(api.Column(*row)) %}
  {% endfor %}
 
  {{ return(columns) }}
{% endmacro %}


{% macro alter_column_type(relation, column_name, new_column_type) -%}
  {{ return(adapter.dispatch('alter_column_type', 'dbt')(relation, column_name, new_column_type)) }}
{% endmacro %}



{% macro diff_columns(source_columns, target_columns) %}

  {% set result = [] %}
  {% set source_names = source_columns | map(attribute = 'column') | list %}
  {% set target_names = target_columns | map(attribute = 'column') | list %}

   {# --check whether the name attribute exists in the target - this does not perform a data type check #}
   {% for sc in source_columns %}

     {% if sc.name not in target_names %}
        {{ result.append(sc) }}
     {% endif %}
   {% endfor %}

  {{ return(result) }}

{% endmacro %}
{% macro diff_column_data_types(source_columns, target_columns) %}

  {% set result = [] %}
  {% for sc in source_columns %}
    {% set tc = target_columns | selectattr("name", "equalto", sc.name) | list | first %}
    {% if tc %}
      {% if sc.data_type != tc.data_type and not sc.can_expand_to(other_column=tc) %}
        {{ result.append( { 'column_name': tc.name, 'new_type': sc.data_type } ) }}
      {% endif %}
    {% endif %}
  {% endfor %}

  {{ return(result) }}

{% endmacro %}







{% macro vertica__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} add column {{ adapter.quote(tmp_column) }} {{ new_column_type }};
    update {{ relation }} set {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
    alter table {{ relation }} drop column {{ adapter.quote(column_name) }} cascade;
    alter table {{ relation }} rename column {{ adapter.quote(tmp_column) }} to {{ adapter.quote(column_name) }}
  {% endcall %}

{% endmacro %}



{% macro alter_relation_add_remove_columns(relation, add_columns, remove_columns ) -%}
  {{ return(adapter.dispatch('alter_relation_add_remove_columns', 'dbt')(relation, add_columns, remove_columns)) }}
{% endmacro %}


{% macro vertica__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}
  {% if remove_columns is none %}
    {% set remove_columns = [] %}
  {% endif %}
            {% for column in add_columns %}
             {% set sql -%}
              alter table {{ relation }}
                   add column {{  adapter.quote(column.name) }} {{ column.data_type }} 
                {%- endset -%} 
               {% do run_query(sql) %}
            {% endfor %}
            {% for column in remove_columns %}  
             {% set sql -%}
              alter table  {{ relation }} 
                drop column {{  adapter.quote(column.name) }}  ;
                 {%- endset -%} 
             
            {% endfor %}
             {% do log(sql) %}
              {% do run_query(sql) %}
              
           
  -- {% do run_query(sql) %}
{% endmacro %}



{# 
  No need to implement get_columns_in_query(). Syntax supported by default. 
#}