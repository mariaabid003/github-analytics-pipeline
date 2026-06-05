-- fact_commits.sql
-- One row per PushEvent — commit activity per repo.
-- Grain: event_id (one push can contain multiple commits).

WITH pushes AS (
    SELECT
        event_id,
        repo_name,
        repo_owner,
        language,
        pushed_by,
        org,
        commit_count,
        commit_messages,
        stars,
        forks,
        is_tracked,
        pushed_at,
        event_date,
        event_hour,
        ingested_at,
        processed_at
    FROM {{ ref('stg_push_events') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['event_id']) }}  AS commit_fact_key,
    event_id,
    repo_name,
    repo_owner,
    language,
    pushed_by,
    org,
    commit_count,
    commit_messages,
    stars,
    forks,
    is_tracked,
    pushed_at,
    event_date,
    event_hour,
    ingested_at,
    processed_at

FROM pushes
