{{ config(
    materialized="table",
    alias="looker_views"
) }}

-- grabbing the type of content
WITH content_id AS (
  SELECT
    CAST(`Event Attribute Value` AS STRING) AS event_source, 
    `Event Name` AS event_name, 
    `User Email` AS user_email, 
    `Event Created Time` AS time_created
  FROM {{ source('Looker', 'looker_events_usage') }}
  WHERE `Event Attribute Name` = 'content_type'
),

-- grabbing the content id 
content_type AS (
  SELECT
    CAST(`Event Attribute Value` AS STRING) AS event_source, 
    `Event Name` AS event_name, 
    `User Email` AS user_email, 
    CAST(`Event Attribute Value` AS STRING) AS content_id_value, 
    `Event Created Time` AS time_created_2
  FROM {{ source('Looker', 'looker_events_usage') }}
  WHERE `Event Attribute Name` = 'content_id'
), 

-- joining these two 
pre_perf AS (
  SELECT 
    content_id.event_name, 
    content_id.user_email, 
    CASE 
      WHEN content_id.event_source = 'looks' THEN 'look'
      WHEN content_id.event_source = 'dashboards-next' THEN 'dashboard'
      ELSE content_id.event_source
    END AS event_source,
    content_id.time_created,
    content_type.content_id_value
  FROM content_id
  LEFT JOIN content_type 
    ON content_id.time_created = content_type.time_created_2 
    AND content_id.user_email = content_type.user_email
), 

-- mapping names
mapping AS (
  SELECT
    COALESCE(`Dashboard Title`, `Look Title`) AS source_name,
    CAST(COALESCE(`Dashboard_ID`, `Look ID`) AS STRING) AS id
  FROM {{ source('Looker', 'looker_name_mapping') }}
  WHERE `Dashboard Title` IS NOT NULL OR `Look Title` IS NOT NULL
),

-- correcting names
correcting_names AS( 
  SELECT 
    pre_perf.event_name, 
    pre_perf.user_email, 
    pre_perf.event_source, 
    pre_perf.time_created, 
    COALESCE(mapping.source_name, pre_perf.content_id_value) AS page_title
  FROM pre_perf
  LEFT JOIN mapping 
    ON pre_perf.content_id_value = mapping.id
),

-- final select with union all
-- view events 
joined AS (
  SELECT 
    CAST(time_created AS TIMESTAMP) AS event_timestamp,
    event_name, 
    user_email, 
    CAST(NULL AS STRING) AS query_explore,
    CAST(NULL AS STRING) AS history_dashboard_session,
    event_source, 
    page_title, 
    'Looker' AS source
  FROM correcting_names

  UNION ALL

  -- joining with the query events 
  SELECT 
    event_timestamp,
    'Query' AS event_name, 
    user_email,
    query_explore,
    history_dashboard_session,
    event_source,
    CAST(page_title AS STRING) AS page_title,
    'Looker' AS source
  FROM {{ ref('looker_queries') }}
),

-- finding known values for page_title
known_values AS (
  SELECT 
    page_title,
    MAX(query_explore) AS known_query_explore
  FROM joined
  GROUP BY page_title
), 

-- replacing null query_explore with known values
final AS (
  SELECT 
    j.event_timestamp,
    j.event_name,
    j.user_email,
    COALESCE(j.query_explore, kv.known_query_explore) AS query_explore,
    j.history_dashboard_session,
    j.event_source,
    j.page_title,
    j.source
  FROM joined j
  LEFT JOIN known_values kv
    ON j.page_title = kv.page_title
)

SELECT * FROM final
