{% macro datediff(first_date, second_date, datepart) -%}

  {% if dbt_version[0] == 1 and dbt_version[2] >= 2 %}
    {{ return(dbt.datediff(first_date, second_date, datepart)) }}
  {% else %}

    datediff( {{datepart}},
    cast({{first_date}} as datetime),
        cast({{second_date}} as datetime)
        
       
    )

  {% endif %}

{%- endmacro %}