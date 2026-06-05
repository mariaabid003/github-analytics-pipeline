-- ─────────────────────────────────────────────────────────────────────────────
-- GitHub Analytics Pipeline — BigQuery Schema
-- Run via: python bigquery/bq_load.py --setup-only
-- ─────────────────────────────────────────────────────────────────────────────


-- ── Raw Events Table (loaded from GCS processed Parquet via bq load) ──────────
CREATE TABLE IF NOT EXISTS `github_raw.raw_github_events`
(
    -- Identifiers
    event_id            STRING      NOT NULL  OPTIONS (description = "GitHub event ID"),
    event_type          STRING               OPTIONS (description = "WatchEvent / ForkEvent / PushEvent / PullRequestEvent / IssuesEvent / ReleaseEvent / CreateEvent / IssueCommentEvent"),
    action              STRING               OPTIONS (description = "Event sub-action: opened, closed, merged, etc."),

    -- Actor
    actor               STRING               OPTIONS (description = "GitHub username who triggered the event"),
    org                 STRING               OPTIONS (description = "Organisation login (if org repo)"),

    -- Repository
    repo_name           STRING      NOT NULL  OPTIONS (description = "owner/repo"),
    repo_owner          STRING               OPTIONS (description = "Repository owner login"),
    language            STRING               OPTIONS (description = "Primary programming language"),
    stars               INT64                OPTIONS (description = "Star count at event time"),
    forks               INT64                OPTIONS (description = "Fork count at event time"),
    open_issues         INT64                OPTIONS (description = "Open issue count at event time"),
    description         STRING               OPTIONS (description = "Repository description (first 200 chars)"),
    is_tracked          BOOL                 OPTIONS (description = "True if repo is in GITHUB_TRACK_REPOS"),

    -- Content
    text_content        STRING               OPTIONS (description = "Commit messages / PR title+body / issue title (500 chars)"),
    commit_count        INT64                OPTIONS (description = "Number of commits in a PushEvent"),
    pr_merged           BOOL                 OPTIONS (description = "True if this PR event is a merge"),
    pr_number           INT64                OPTIONS (description = "Pull request number"),
    issue_number        INT64                OPTIONS (description = "Issue number"),
    ref_type            STRING               OPTIONS (description = "branch or tag (for CreateEvent)"),
    ref_name            STRING               OPTIONS (description = "Branch or tag name"),

    -- Derived (added by batch processor)
    is_merge            BOOL                 OPTIONS (description = "True if PullRequestEvent AND pr_merged"),
    has_text            BOOL                 OPTIONS (description = "True if text_content is non-empty"),
    pr_state            STRING               OPTIONS (description = "open / closed / merged (PullRequestEvent only)"),

    -- Timestamps
    created_at          TIMESTAMP            OPTIONS (description = "GitHub event creation timestamp"),
    ingested_at         TIMESTAMP            OPTIONS (description = "Kafka ingestion timestamp"),
    processed_at        TIMESTAMP            OPTIONS (description = "Batch processor timestamp"),
    date_partition      DATE        NOT NULL  OPTIONS (description = "Partition key — event date")
)
PARTITION BY date_partition
CLUSTER BY event_type, repo_name, language
OPTIONS (
    description = "Raw GitHub events loaded from GCS processed Parquet. One row per GitHub event."
);
