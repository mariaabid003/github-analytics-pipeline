-- fact_prs.sql
-- One row per PullRequestEvent.
-- A single PR (pr_number) can appear multiple times as it transitions state.
-- Grain: event_id (each state change is its own fact row).

WITH prs AS (
    SELECT
        event_id,
        repo_name,
        repo_owner,
        language,
        pr_actor,
        org,
        pr_action,
        pr_number,
        pr_merged,
        is_merge,
        -- pr_state (Spark-derived) is intentionally excluded: pr_status is
        -- recomputed from pr_action + is_merge in the final SELECT below.
        pr_title_body,
        has_text,
        stars,
        forks,
        is_tracked,
        event_at,
        event_date,
        event_hour,
        ingested_at,
        processed_at
    FROM {{ ref('stg_pr_events') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['event_id']) }}  AS pr_fact_key,
    event_id,
    repo_name,
    repo_owner,
    language,
    pr_actor,
    org,
    pr_action,
    pr_number,
    pr_merged,
    is_merge,

    -- Friendly status bucket.
    -- Any action that isn't a close/merge keeps the PR in 'open' state
    -- (synchronize, labeled, unlabeled, review_requested, etc.).
    CASE
        WHEN is_merge             THEN 'merged'
        WHEN pr_action = 'closed' THEN 'closed'
        ELSE 'open'
    END AS pr_status,

    pr_title_body,
    has_text,
    stars,
    forks,
    is_tracked,
    event_at,
    event_date,
    event_hour,
    ingested_at,
    processed_at

FROM prs
