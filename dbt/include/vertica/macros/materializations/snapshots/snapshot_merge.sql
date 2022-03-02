{% macro vertica__snapshot_merge_sql(target, source, insert_cols_csv) -%}

  {% if insert_cols %}
      {%- set insert_cols_csv = insert_cols | join(', ') -%}
  {% else %}
      {%- set insert_cols_csv = get_quoted_csv(get_columns_in_relation(target) | map(attribute="name")) %}
  {% endif %}

  merge into {{ target }} as DBT_INTERNAL_DEST
  using {{ source }} as DBT_INTERNAL_SOURCE
  on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

  when matched
    and DBT_INTERNAL_DEST.dbt_valid_to is null
    and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
      then update
      set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

  when not matched
    and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
      then insert ({{ insert_cols_csv }})
      values (
        {% for column in vertica__get_columns_in_relation(target) | map(attribute="name") -%}
          DBT_INTERNAL_SOURCE.{{ column }}
          {%- if not loop.last %}, {% endif %}
        {%- endfor %}
      )

{% endmacro %}