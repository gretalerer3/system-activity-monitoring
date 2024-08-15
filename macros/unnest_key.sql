{% macro unnest_key(unnest_field, key_name, value_type) %}

    (SELECT 
        value.{{value_type}}
    FROM UNNEST({{unnest_field}})
    WHERE TRUE 
        AND key = '{{key_name}}')

{% endmacro %}
