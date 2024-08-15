{% macro content_type() %}
CASE 
    WHEN {{ 'history_source' }} = 'dashboard' THEN CAST(dashboard_title AS STRING)
    WHEN {{ 'history_source'}} = 'look' THEN look_title
    WHEN {{ 'history_source' }} = 'explore' THEN query_explore
    ELSE query_explore
END
{% endmacro %}
