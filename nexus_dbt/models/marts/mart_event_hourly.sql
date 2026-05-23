{{
    config(
        materialized='table',
        order_by='(event_hour)',
        settings={'allow_nullable_key': 1}
    )
}}

SELECT
    event_hour,
    count()                AS total_events,
    uniq(repo_name)        AS active_repos,
    uniq(actor_login)      AS active_contributors,
    countIf(event_type = 'pushevent')        AS pushes,
    countIf(event_type = 'pullrequestevent') AS pull_requests,
    countIf(event_type = 'watchevent')       AS stars,
    countIf(event_type = 'forkevent')        AS forks
FROM {{ ref('stg_github_events') }}
GROUP BY event_hour
ORDER BY event_hour DESC