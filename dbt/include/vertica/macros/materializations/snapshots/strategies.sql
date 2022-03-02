{#
    Create SCD Hash SQL fields cross-db
#}
{% macro vertica__snapshot_hash_arguments(args) -%}
    md5({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}


{#
    Get the current time cross-db
#}
{% macro vertica__snapshot_get_time() -%}
  {{ current_timestamp() }}
{%- endmacro %}


{% macro vertica__snapshot_string_as_time(timestamp) %}
    {%- set result = "('" ~ timestamp ~ "'::timestamptz)" -%}
    {{ return(result) }}
{% endmacro %}