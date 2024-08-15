{{ config(
    materialized="table",
    alias="global"
) }}

WITH uniting AS(
SELECT
    event_timestamp
    , event_name
    , user_email
    , NULL AS query_explore  
    , session_id as history_dashboard_session
    , 'Report' as event_source -- Because we are only looking at reports in Looker Studio
    , page_title 
    , 'Looker Studio' AS source

FROM {{ref('looker_studio')}}


UNION ALL 

SELECT 
  event_timestamp
  , event_name 
  , user_email
  , query_explore
  , history_dashboard_session
  , event_source
  , page_title
  -- , min_event
  -- , max_event
  -- , time_diff
  , source
FROM {{ ref('looker_queries') }}
), 

date_calculations AS (
  SELECT 
  history_dashboard_session
  , MIN(event_timestamp) AS min_event_time
  , MAX(event_timestamp) AS max_event_time
  , DATETIME_DIFF(MAX(event_timestamp), MIN(event_timestamp), MINUTE) as time_diff --minutes 
  FROM uniting
  GROUP BY history_dashboard_session
)

SELECT 
    a.event_timestamp
  , a.event_name 
  , a.user_email
  , a.query_explore
  , a.history_dashboard_session
  , a.event_source
  , a.page_title
  , a.source
  , b.min_event_time
  , b.max_event_time
  , b.time_diff
  FROM uniting a

jOIN date_calculations b 
ON a.history_dashboard_session = b.history_dashboard_session

