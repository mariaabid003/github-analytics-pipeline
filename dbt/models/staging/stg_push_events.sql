-- stg_push_events.sql
-- PushEvent records with commit metadata.
-- Grain: one row per PushEvent (may contain multiple commits).

SELECT
    event_id,
    repo_name,
    repo_owner,
    language,
    actor                       AS pushed_by,
    org,
    commit_count,
    text_content                AS commit_messages,  -- pipe-separated first 5 messages
    stars,
    forks,
    is_tracked,
    created_at                  AS pushed_at,
    event_date,
    event_hour,
    ingested_at,
    processed_at

FROM {{ ref('stg_github_events') }}
WHERE event_type = 'PushEvent'
-- Removed commit_count > 0: the GitHub public timeline API often returns
-- PushEvents with an empty commits array (commit_count = 0) even for real pushes.
-- The actual commit total is in the 'size' field which the producer now captures.
-- Filtering on > 0 was silently dropping all public-timeline push events.
