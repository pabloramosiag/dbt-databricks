{% macro transform_poll_results(column_name) %}
    CASE
        WHEN {{ column_name }} = '[]' THEN NULL
        ELSE {{ column_name }}
    END AS {{ column_name }}
{% endmacro %}
