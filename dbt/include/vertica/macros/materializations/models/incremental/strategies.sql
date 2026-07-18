



{% macro get_incremental_append_sql(target_relation, temp_relation, dest_columns) %}

  {{ return(adapter.dispatch('get_incremental_append_sql', 'dbt')(target_relation, temp_relation, dest_columns)) }}

{% endmacro %}


{% macro vertica__get_incremental_append_sql(target_relation, tmp_relation,dest_columns) %}

  {% do return(get_insert_into_sql(target_relation, tmp_relation,  dest_columns)) %}

{% endmacro %}
{% macro get_incremental_delete_insert_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_delete_insert_sql', 'dbt')(arg_dict)) }}

{% endmacro %}


{% macro vertica__get_incremental_delete_insert_sql(arg_dict) %}

  {% do return(get_delete_insert_merge_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["unique_key"], arg_dict["dest_columns"])) %}

{% endmacro %}

{% macro get_incremental_merge_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_merge_sql', 'dbt')(arg_dict)) }}

{% endmacro %}

{% macro vertica__get_incremental_merge_sql(arg_dict) %}

  {% do return(get_merge_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["unique_key"], arg_dict["dest_columns"])) %}

{% endmacro %}





{% macro get_incremental_insert_overwrite_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_insert_overwrite_sql', 'dbt')(arg_dict)) }}

{% endmacro %}


{% macro vertica__get_incremental_insert_overwrite_sql(arg_dict) %}

  {% do return(get_insert_overwrite_merge_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"], arg_dict["predicates"])) %}

{% endmacro %}






{% macro vertica__get_incremental_microbatch_sql(arg_dict) %}
  {%- set target = arg_dict["target_relation"] -%}
  {%- set source = arg_dict["temp_relation"] -%}
  {%- set dest_columns = arg_dict["dest_columns"] -%}
  {%- set predicates = [] -%}

  {% if not model.config.get("__dbt_internal_microbatch_event_time_start") or not model.config.get("__dbt_internal_microbatch_event_time_end") -%}
    {% do exceptions.raise_compiler_error("dbt could not compute the start and end timestamps for the running batch") %}
  {% endif %}

  {#-- Build the event-time window predicates for this batch --#}
  {% set start_time = model.config["__dbt_internal_microbatch_event_time_start"] %}
  {% do predicates.append(model.config.event_time ~ " >= TIMESTAMP '" ~ start_time ~ "'") %}
  {% set end_time = model.config["__dbt_internal_microbatch_event_time_end"] %}
  {% do predicates.append(model.config.event_time ~ " < TIMESTAMP '" ~ end_time ~ "'") %}
  {% do arg_dict.update({'incremental_predicates': predicates}) %}

  delete from {{ target }} where (
  {% for predicate in predicates %}
    {%- if not loop.first %} and {% endif -%} {{ predicate }}
  {% endfor %}
  );

  {{ get_insert_into_sql(target, source, dest_columns) }}

{% endmacro %}


{% macro get_incremental_default_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_default_sql', 'dbt')(arg_dict)) }}

{% endmacro %}




{% macro vertica__get_incremental_default_sql(target_relation, tmp_relation,  dest_columns) %}

  {% do return(get_incremental_append_sql(target_relation, tmp_relation, dest_columns)) %}

{% endmacro %}



{% macro default__get_incremental_default_sql(arg_dict) %}

  {% do return(get_incremental_append_sql(arg_dict)) %}

{% endmacro %}


{% macro get_insert_into_sql(target_relation, temp_relation, dest_columns) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ temp_relation }}
    )

{% endmacro %}
