{%- macro vertica__get_grant_sql(relation, privilege, grantees) -%}
    grant {{ privilege }} on {{ relation }} to {{ grantees|join(",") }}
{%- endmacro %}

{% macro vertica__get_show_grant_sql(relation) %}
    select privileges_description,grantee from grants where object_type = '{{relation.type}}' and object_name= '{{ relation.identifier }}' and grantee != session_user()
{% endmacro %}

{%- macro vertica__get_revoke_sql(relation, privilege, grantees) -%}
    revoke {{ privilege }} on {{ relation }} from {{ adapter.quote(grantees[0]) }}
{%- endmacro %}


{% macro vertica__do_apply_grants(relation, grant_config) %}
    {% for privilege, grantees in grant_config.items() %}
        grant {{ privilege }} on {{ relation }} to {{ grantees|join(",") }};
    {% endfor %}
{% endmacro %}
