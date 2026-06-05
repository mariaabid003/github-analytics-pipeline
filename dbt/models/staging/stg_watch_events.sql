-- stg_watch_events.sql
-- WatchEvent (star) records.
-- Grain: one row per star event — who starred which repo at what time.

SELECT
    event_id,
    repo_name,
    repo_owner,
    language,
    actor                       AS starred_by,
    org,
    stars,
    forks,
    is_tracked,
    created_at                  AS starred_at,
    event_date,
    event_hour,
    ingested_at,
    processed_at

FROM {{ ref('stg_github_events') }}
WHERE event_type = 'WatchEvent'
