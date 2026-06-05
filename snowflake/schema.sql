-- ─────────────────────────────────────────────────────────────────────────────
-- GitHub Analytics Pipeline — Snowflake Schema
-- Run via: python snowflake/sf_load.py --setup-only
-- Placeholders {SF_DATABASE} and {SF_SCHEMA_RAW} are replaced at runtime.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Raw Events Table ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA_RAW}.RAW_GITHUB_EVENTS
(
    -- Identifiers
    EVENT_ID            STRING        NOT NULL COMMENT 'GitHub event ID',
    EVENT_TYPE          STRING                 COMMENT 'WatchEvent / ForkEvent / PushEvent / PullRequestEvent / IssuesEvent / ReleaseEvent / CreateEvent / IssueCommentEvent',
    ACTION              STRING                 COMMENT 'Event sub-action: opened, closed, merged, etc.',

    -- Actor
    ACTOR               STRING                 COMMENT 'GitHub username who triggered the event',
    ORG                 STRING                 COMMENT 'Organisation login (if org repo)',

    -- Repository
    REPO_NAME           STRING        NOT NULL COMMENT 'owner/repo',
    REPO_OWNER          STRING                 COMMENT 'Repository owner login',
    LANGUAGE            STRING                 COMMENT 'Primary programming language',
    STARS               NUMBER(12,0)           COMMENT 'Star count at event time',
    FORKS               NUMBER(12,0)           COMMENT 'Fork count at event time',
    OPEN_ISSUES         NUMBER(12,0)           COMMENT 'Open issue count at event time',
    DESCRIPTION         STRING                 COMMENT 'Repository description (first 200 chars)',
    IS_TRACKED          BOOLEAN                COMMENT 'True if repo is in GITHUB_TRACK_REPOS',
    TOPICS              VARIANT                COMMENT 'Array of repo topics',

    -- Content
    TEXT_CONTENT        STRING                 COMMENT 'Commit messages / PR title+body / issue title (500 chars)',
    COMMIT_COUNT        NUMBER(10,0)           COMMENT 'Number of commits in a PushEvent',
    PR_MERGED           BOOLEAN                COMMENT 'True if this PR event is a merge',
    PR_NUMBER           NUMBER(12,0)           COMMENT 'Pull request number',
    ISSUE_NUMBER        NUMBER(12,0)           COMMENT 'Issue number',
    REF_TYPE            STRING                 COMMENT 'branch or tag (for CreateEvent)',
    REF_NAME            STRING                 COMMENT 'Branch or tag name',

    -- Derived (added by batch processor)
    IS_MERGE            BOOLEAN                COMMENT 'True if PullRequestEvent AND pr_merged',
    HAS_TEXT            BOOLEAN                COMMENT 'True if text_content is non-empty',
    PR_STATE            STRING                 COMMENT 'open / closed / merged (PullRequestEvent only)',

    -- Timestamps
    CREATED_AT          TIMESTAMP_TZ           COMMENT 'GitHub event creation timestamp',
    INGESTED_AT         TIMESTAMP_TZ           COMMENT 'Kafka ingestion timestamp',
    PROCESSED_AT        TIMESTAMP_TZ           COMMENT 'Batch processor timestamp',
    DATE_PARTITION      DATE          NOT NULL  COMMENT 'Partition key — event date'
)
CLUSTER BY (DATE_PARTITION, EVENT_TYPE)
COMMENT = 'Raw GitHub events loaded from local processed Parquet. One row per GitHub event.';
