 {%- macro type_string() -%} 
   {{ return(adapter.dispatch('type_string', 'dbt')()) }} 
 {%- endmacro -%} 
  
 {% macro default__type_string() %} 
     {{ return(api.Column.translate_type("string")) }} 
 {% endmacro %}

{% macro vertica__type_string() %} 
     {{ return(api.Column.translate_type("string")) }} 
 {% endmacro %}
