{% macro vertica__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% set sql %}
    create table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
  {% endset %}

  {{ return(sql) }}
{% endmacro %}


{% macro vertica__load_csv_rows(model, agate_table) %}
  {{ return(copy_local_load_csv_rows(model, agate_table) )}}
{% endmacro %}


{% macro basic_load_csv_rows(model, batch_size, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% do bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            {% for row in chunk -%}

                insert into {{ this.render() }} ({{ cols_sql }}) values
                ({%- for column in agate_table.column_names -%} 
                  %s 
                  {%- if not loop.last %},{% endif %} 
                {%- endfor %});
                
            {% endfor %}
        {% endset %}

        {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% do statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}


{% macro copy_local_load_csv_rows(model, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}

    {% set sql %}
        copy {{ this.render() }}
        ({{ cols_sql }})
        from local '{{ agate_table.original_abspath }}'
        delimiter ','
        enclosed by '"'
        skip 1
        abort on error
        rejected data as table {{ this.without_identifier() }}.seed_rejects;
    {% endset %}

    {{ return(sql) }}
{% endmacro %}


{# 
  No need to implement reset_csv_table(). Syntax supported by default. 
  No need to implement get_binding_char(). Syntax supported by default.
  No need to implement get_batch_size(). Syntax supported by default.
  No need to implement get_seed_column_quoted_csv(). Syntax supported by default.
#}