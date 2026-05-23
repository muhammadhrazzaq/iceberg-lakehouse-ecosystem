
{{
    config(
        materialized='table',
        order_by='(repo_name, event_date)',
        partition_by='toYYYYMM(event_date)',
          settings={'allow_nullable_key': 1}
    )
}}

SELECT
    repo_name,
    event_date,
    countIf(event_type = 'pushevent')         AS pushes,
    countIf(event_type = 'pullrequestevent')  AS pull_requests,
    countIf(event_type = 'watchevent')        AS stars_today,
    countIf(event_type = 'forkevent')         AS forks,
    countIf(event_type = 'issuesevent')       AS issues,
    uniq(actor_login)                         AS unique_contributors,
    count()                                   AS total_events,
    max(stars)                                AS current_stars
FROM {{ ref('stg_github_events') }}
GROUP BY repo_name, event_date
ORDER BY event_date DESC, total_events DESC
