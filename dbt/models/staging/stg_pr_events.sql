-- stg_pr_events.sql
-- PullRequestEvent records with full lifecycle state.
-- Grain: one row per PR event (a single PR can have multiple events: opened, closed, merged).

SELECT
    event_id,
    repo_name,
    repo_owner,
    language,
    actor                       AS pr_actor,
    org,
    action                      AS pr_action,
    pr_number,
    pr_merged,
    is_merge,
    pr_state,                   -- open | closed | merged (derived by batch processor)
    text_content                AS pr_title_body,
    has_text,
    stars,
    forks,
    is_tracked,
    created_at                  AS event_at,
    event_date,
    event_hour,
    ingested_at,
    processed_at

FROM {{ ref('stg_github_events') }}
WHERE event_type = 'PullRequestEvent'
