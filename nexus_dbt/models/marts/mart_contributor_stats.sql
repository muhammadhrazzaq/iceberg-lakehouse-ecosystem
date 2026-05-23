{{
    config(
        materialized='table',
        order_by='(actor_login, event_date)',
        settings={'allow_nullable_key': 1}
    )
}}

WITH daily AS (
    SELECT
        actor_login,
        event_date,
        count()          AS daily_events,
        uniq(repo_name)  AS repos_active_in
    FROM {{ ref('stg_github_events') }}
    GROUP BY actor_login, event_date
)
SELECT
    actor_login,
    event_date,
    daily_events,
    repos_active_in,
    sum(daily_events) OVER (
        PARTITION BY actor_login
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS events_7d_rolling,
    avg(daily_events) OVER (
        PARTITION BY actor_login
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_7d,
    round(
        (daily_events - avg(daily_events) OVER (
            PARTITION BY actor_login
            ORDER BY event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        )) / nullIf(stddevPop(daily_events) OVER (
            PARTITION BY actor_login
            ORDER BY event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ), 0),
    2) AS z_score
FROM daily
ORDER BY event_date DESC, daily_events DESC
