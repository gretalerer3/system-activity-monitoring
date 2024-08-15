{{ config(
    materialized="table",
    alias="looker_interim"
) }}

WITH query_data AS (
    SELECT 
        'Query' AS event_name 
        , FORMAT_TIMESTAMP('%Y-%m-%d', history_created_time) AS event_date
        , history_created_time AS event_timestamp
        , user_email as user_email
        , query_explore as query_explore
        , history_dashboard_session as history_dashboard_session
        , history_source as event_source
        , {{ content_type() }} as page_title
        , 'Looker' AS source


    FROM {{ source('Looker', 'looker_query_usage') }}
)

SELECT * FROM query_data


