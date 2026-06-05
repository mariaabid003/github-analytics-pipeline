-- dim_repos.sql
-- One row per unique repository.
-- Uses the most recent event per repo to snapshot current metadata.
-- Grain: repo_name (unique).

WITH ranked AS (
    SELECT
        repo_name,
        repo_owner,
        language,
        stars,         -- already BIGINT from stg_github_events
        forks,         -- already BIGINT from stg_github_events
        open_issues,   -- already BIGINT from stg_github_events
        description,
        is_tracked,
        created_at                      AS last_seen_at,
        ROW_NUMBER() OVER (
            PARTITION BY repo_name
            ORDER BY created_at DESC
        ) AS rn
    FROM {{ ref('stg_github_events') }}
    WHERE repo_name IS NOT NULL
),

activity AS (
    SELECT
        repo_name,
        COUNT(DISTINCT event_id)                                        AS total_events,
        MIN(created_at)                                                 AS first_seen_at,
        MAX(created_at)                                                 AS latest_event_at,
        SUM(CASE WHEN event_type = 'WatchEvent'       THEN 1 ELSE 0 END) AS total_stars,
        SUM(CASE WHEN event_type = 'ForkEvent'        THEN 1 ELSE 0 END) AS total_forks_events,
        SUM(CASE WHEN event_type = 'PushEvent'        THEN 1 ELSE 0 END) AS total_pushes,
        SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS total_pr_events,
        -- Exclude '[unknown]' placeholder so inflated contributor counts don't appear.
        COUNT(DISTINCT CASE WHEN actor != '[unknown]' THEN actor END)   AS unique_contributors
    FROM {{ ref('stg_github_events') }}
    WHERE repo_name IS NOT NULL
    GROUP BY repo_name
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['r.repo_name']) }} AS repo_key,
    r.repo_name,
    r.repo_owner,
    r.language,
    r.stars,
    r.forks,
    r.open_issues,
    r.description,
    r.is_tracked,
    r.last_seen_at,

    a.total_events,
    a.first_seen_at,
    a.latest_event_at,
    a.total_stars,
    a.total_forks_events,
    a.total_pushes,
    a.total_pr_events,
    a.unique_contributors

FROM ranked r
INNER JOIN activity a ON r.repo_name = a.repo_name
WHERE r.rn = 1
