
{% macro vertica__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}


{% macro vertica__create_or_replace_clone(this_relation, defer_relation) %}
    SELECT COPY_TABLE ( {{ "'"+defer_relation|replace('"','')+"'"}}, {{ "'"+this_relation|replace('"','')+"'"}} );
{% endmacro %}