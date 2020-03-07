{% materialization view, adapter='vertica' %}
  {{ return(create_or_replace_view()) }}
{% endmaterialization %}
