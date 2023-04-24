{% materialization incremental, adapter='vertica' %}

  {% set unique_key = config.get('unique_key')   or 'none' %}
  {% set grant_config = config.get('grants') %}
  {% set target_relation = this %}
  -- {%- set existing_relation = load_cached_relation(this) -%}
  {% set existing_relation = load_relation(this) %}
   {%- set target_relation = this.incorporate(type='table') -%}
  {% set tmp_relation = make_temp_relation(target_relation) %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}


  -- {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = vertica__validate_get_incremental_strategy(config) %}

  

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}


  -- {# -- first check whether we want to full refresh for source view or config reasons #}
  {% set trigger_full_refresh = (full_refresh_mode or existing_relation.is_view) %}


  {% if existing_relation is none %}
      {% set build_sql = vertica__create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
      -- {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}
      {% do adapter.rename_relation(existing_relation, backup_relation) %}
      {% set build_sql = vertica__create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% elif full_refresh_mode %}
      -- {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set tmp_identifier = model['name'] + '__dbt_tmp' %}
      {% set backup_identifier = model['name'] + '__dbt_backup' %}
      {% set intermediate_relation = existing_relation.incorporate(path={"identifier": tmp_identifier}) %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      
      {% set build_sql = vertica__create_table_as(False, intermediate_relation, sql) %}
      
      {% set need_swap = true %}
      {% do to_drop.append(backup_relation) %}
      {% do to_drop.append(intermediate_relation) %}
  {% else %}
      {% do run_query(vertica__create_table_as(True, tmp_relation, sql)) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      -- {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
         {% set msg %}
       sachin: {{tmp_relation}}
    {% endset %}
  {% do log(msg) %}
      {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, target_relation) %}
      {% if not dest_columns %}
      {%  set dest_columns =  vertica__get_columns_in_relation(existing_relation)%}
        -- {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
      {% endif %}


      {% set build_sql = vertica__get_incremental_sql(strategy, target_relation, tmp_relation, unique_key, dest_columns) %}


  {% endif %}
  {% call statement("main") %}
      {{ build_sql }}
      {% if grant_config is not none %}
       ; {{ vertica__do_apply_grants(target_relation, grant_config) }}
      {% endif %}
  {% endcall %}
  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
  {% endif %}
  {% do persist_docs(target_relation, model) %}
  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}