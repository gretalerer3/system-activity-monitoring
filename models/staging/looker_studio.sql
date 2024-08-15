{{ config(
    materialized="table",
    alias="looker_studio"
) }}

WITH a AS (
    SELECT 
      TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), SECOND) AS event_timestamp
    , event_name
    , user_pseudo_id as user_email
    ,CONCAT(SPLIT(user_pseudo_id, '.')[OFFSET(0)], {{ unnest_key('event_params', 'ga_session_id', 'int_value') }}) AS session_id
    , {{ unnest_key('event_params', 'page_location', 'string_value') }} AS page_location
    , {{ unnest_key('event_params', 'page_title', 'string_value') }} AS page_title
    , 'Looker Studio' AS source
    , 'report' as event_source -- Because we are only looking at reports in Looker Studio
    , NULL AS query_explore
    FROM {{ source('Looker Studio', 'looker_studio_query_usage') }}
    -- agregar para calcular session duration 
)

SELECT * FROM a
