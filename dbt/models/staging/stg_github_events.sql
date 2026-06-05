-- stg_github_events.sql
-- Base staging model: cleans and types ALL raw events.
-- All other staging models filter from this one.
--
-- Adapter routing (via cross_db.sql macros):
--   dev  (DuckDB)    → reads from seed {{ ref('raw_github_events') }}
--   prod (Snowflake) → reads from {{ source('github_raw', 'raw_github_events') }}

WITH source AS (
    SELECT * FROM {{ github_raw_source() }}
),

-- Deduplicate on event_id: keep the most recently ingested copy.
-- Duplicate event_ids arise when the Snowflake loader re-processes
-- a date partition that was already loaded (e.g. after a backfill).
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at DESC NULLS LAST
        ) AS _row_num
    FROM source
    WHERE event_id IS NOT NULL
),

cleaned AS (
    SELECT
        -- Identifiers
        event_id,
        event_type,
        COALESCE(action, '')                                AS action,

        -- Actor
        COALESCE(actor, '[unknown]')                        AS actor,
        COALESCE(org, '')                                   AS org,

        -- Repository
        repo_name,
        SPLIT_PART(repo_name, '/', 1)                       AS repo_owner,
        COALESCE(language, 'Unknown')                       AS language,
        COALESCE({{ safe_to_int('stars') }}, 0)             AS stars,
        COALESCE({{ safe_to_int('forks') }}, 0)             AS forks,
        COALESCE({{ safe_to_int('open_issues') }}, 0)       AS open_issues,
        COALESCE(description, '')                           AS description,
        COALESCE(CAST(is_tracked AS BOOLEAN), FALSE)        AS is_tracked,

        -- Content
        COALESCE(text_content, '')                          AS text_content,
        COALESCE({{ safe_to_int('commit_count') }}, 0)      AS commit_count,
        COALESCE(CAST(pr_merged AS BOOLEAN), FALSE)         AS pr_merged,
        {{ safe_to_int('pr_number') }}                      AS pr_number,
        {{ safe_to_int('issue_number') }}                   AS issue_number,
        COALESCE(ref_type, '')                              AS ref_type,
        COALESCE(ref_name, '')                              AS ref_name,

        -- Derived fields (populated by spark_batch/github_batch_processor.py)
        COALESCE(CAST(is_merge AS BOOLEAN), FALSE)          AS is_merge,
        COALESCE(CAST(has_text AS BOOLEAN), FALSE)          AS has_text,
        COALESCE(pr_state, '')                              AS pr_state,

        -- Timestamps (cross-DB: DuckDB TRY_CAST TIMESTAMPTZ / Snowflake TRY_TO_TIMESTAMP_TZ)
        {{ safe_to_timestamp('created_at') }}               AS created_at,
        {{ safe_to_date('created_at') }}                    AS event_date,
        DATE_PART('hour', {{ safe_to_timestamp('created_at') }}) AS event_hour,
        {{ safe_to_timestamp('ingested_at') }}              AS ingested_at,
        {{ safe_to_timestamp('processed_at') }}             AS processed_at,
        {{ safe_to_date('date_partition') }}                AS date_partition

    FROM deduped
    WHERE
        _row_num = 1
        AND event_type IS NOT NULL
        AND repo_name  IS NOT NULL
        -- Filter rows whose timestamp string exists but fails to parse —
        -- those would produce NULL created_at and break not_null tests
        -- downstream (first_seen_at, last_seen_at, event_at).
        AND {{ safe_to_timestamp('created_at') }} IS NOT NULL
        AND {{ safe_to_date('created_at') }} >= CAST('2000-01-01' AS DATE)
        AND {{ safe_to_date('created_at') }} <= CAST('2100-01-01' AS DATE)
)

SELECT * FROM cleaned
