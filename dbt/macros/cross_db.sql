{#
  cross_db.sql — Cross-database compatibility macros
  ====================================================
  Provides adapter-aware SQL functions that work in both:
    - DuckDB  (dev  target — local, no credentials needed)
    - Snowflake (prod target — production warehouse)

  Usage:
    {{ safe_to_timestamp('created_at') }}    → TIMESTAMPTZ cast
    {{ safe_to_date('created_at') }}         → DATE cast
    {{ safe_to_int('stars') }}               → BIGINT / NUMBER cast
    {{ github_raw_source() }}                → correct source ref per target
#}

{#
  Snowflake note — why we use plain ::TYPE casts instead of TRY_TO_*:
  ─────────────────────────────────────────────────────────────────────
  The Parquet COPY INTO loader stores all columns already typed
  (created_at → TIMESTAMP_TZ, stars → NUMBER, date_partition → DATE).
  Snowflake's TRY_TO_TIMESTAMP_TZ / TRY_TO_DATE / TRY_TO_NUMBER internally
  call TRY_CAST, and Snowflake rejects TRY_CAST when the source and target
  types are the same (e.g. TIMESTAMP_TZ → TIMESTAMP_TZ), throwing:
    "Function TRY_CAST cannot be used with arguments of types X and X"
  Plain CAST / :: syntax works for same-type and cross-type conversions
  and is safe here because all raw table columns are already valid and typed.
#}

{# ─── Timestamp cast (timezone-aware) ──────────────────────────────────────── #}
{% macro safe_to_timestamp(field) %}
  {% if target.type == 'duckdb' %}
    TRY_CAST({{ field }} AS TIMESTAMPTZ)
  {% else %}
    CAST({{ field }} AS TIMESTAMP_TZ)
  {% endif %}
{% endmacro %}

{# ─── Date cast ─────────────────────────────────────────────────────────────── #}
{% macro safe_to_date(field) %}
  {% if target.type == 'duckdb' %}
    TRY_CAST({{ field }} AS DATE)
  {% else %}
    CAST({{ field }} AS DATE)
  {% endif %}
{% endmacro %}

{# ─── Integer / large number cast ───────────────────────────────────────────── #}
{#
  Snowflake: use BIGINT (not NUMBER) so Power BI maps the column to
  "Whole Number" instead of "Decimal Number". Snowflake stores BIGINT as
  NUMBER(38,0) internally but reports the type as INTEGER to clients,
  which Power BI correctly maps to Whole Number in both Import and DirectQuery.
#}
{% macro safe_to_int(field) %}
  {% if target.type == 'duckdb' %}
    TRY_CAST({{ field }} AS BIGINT)
  {% else %}
    CAST({{ field }} AS BIGINT)
  {% endif %}
{% endmacro %}

{# ─── Source router: seed in dev, Snowflake table in prod ───────────────────── #}
{# Use as: SELECT * FROM {{ github_raw_source() }}                              #}
{% macro github_raw_source() %}
  {% if target.type == 'duckdb' %}
    {{ ref('raw_github_events') }}
  {% else %}
    {{ source('github_raw', 'raw_github_events') }}
  {% endif %}
{% endmacro %}
