-- dim_contributors.sql
-- One row per unique GitHub actor across all tracked repos.
-- Grain: actor (unique GitHub username).

WITH base AS (
    SELECT
        actor,
        event_id,
        repo_name,
        event_type,
        commit_count,
        is_merge,
        language,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY created_at DESC) AS rn
    FROM {{ ref('stg_github_events') }}
    WHERE
        actor IS NOT NULL
        AND actor != ''
        AND actor != '[unknown]'
),

agg AS (
    SELECT
        actor,

        -- Overall activity
        COUNT(DISTINCT event_id)                                          AS total_events,
        COUNT(DISTINCT repo_name)                                         AS repos_contributed_to,
        MIN(created_at)                                                   AS first_seen_at,
        MAX(created_at)                                                   AS last_seen_at,

        -- Event type breakdown
        SUM(CASE WHEN event_type = 'PushEvent'        THEN 1 ELSE 0 END) AS push_events,
        SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS pr_events,
        SUM(CASE WHEN event_type = 'IssuesEvent'      THEN 1 ELSE 0 END) AS issue_events,
        SUM(CASE WHEN event_type = 'WatchEvent'       THEN 1 ELSE 0 END) AS watch_events,
        SUM(CASE WHEN event_type = 'ForkEvent'        THEN 1 ELSE 0 END) AS fork_events,
        SUM(CASE WHEN event_type = 'IssueCommentEvent' THEN 1 ELSE 0 END) AS comment_events,
        SUM(CASE WHEN event_type = 'ReleaseEvent'     THEN 1 ELSE 0 END) AS release_events,

        -- Commit activity (commit_count is already BIGINT from stg_github_events)
        SUM(commit_count)                                                 AS total_commits,

        -- PR activity
        SUM(CASE WHEN is_merge = TRUE THEN 1 ELSE 0 END)                  AS prs_merged

    FROM base
    GROUP BY actor
),

-- Most recent language and repo per actor (using rn = 1 from base)
latest AS (
    SELECT actor, language AS primary_language, repo_name AS most_recent_repo
    FROM base
    WHERE rn = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['agg.actor']) }} AS contributor_key,
    agg.actor,
    agg.total_events,
    agg.repos_contributed_to,
    agg.first_seen_at,
    agg.last_seen_at,
    agg.push_events,
    agg.pr_events,
    agg.issue_events,
    agg.watch_events,
    agg.fork_events,
    agg.comment_events,
    agg.release_events,
    agg.total_commits,
    agg.prs_merged,
    latest.primary_language,
    latest.most_recent_repo

FROM agg
INNER JOIN latest ON agg.actor = latest.actor
