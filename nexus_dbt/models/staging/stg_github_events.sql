{{
    config(
        materialized='view'
    )
}}

SELECT
    event_id,
    lower(trim(event_type))              AS event_type,
    lower(trim(repo_name))               AS repo_name,
    lower(trim(actor_login))             AS actor_login,
    toDateTime(created_at)               AS created_at,
    toDate(created_at)                   AS event_date,
    toStartOfHour(toDateTime(created_at)) AS event_hour,
    coalesce(stars, 0)                   AS stars
FROM {{ github_events_source() }}
WHERE event_id   IS NOT NULL
  AND created_at IS NOT NULL
  AND repo_name  IS NOT NULL