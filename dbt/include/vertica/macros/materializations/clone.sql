


{% macro vertica__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}


{% macro vertica__create_or_replace_clone(this_relation, defer_relation) %}
    create table
        {{ this_relation }} as select * from  {{ defer_relation }}
     
{% endmacro %}