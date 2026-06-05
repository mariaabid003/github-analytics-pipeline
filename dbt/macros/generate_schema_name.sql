{#
  generate_schema_name.sql — Custom schema name resolution
  =========================================================
  By default dbt generates schema names as:
    {target_schema}_{custom_schema}   e.g. MART_staging, MART_mart

  This override uses the custom schema name directly, giving clean names:
    staging  → STAGING (Snowflake) / staging (DuckDB)
    mart     → MART    (Snowflake) / mart    (DuckDB)

  Seeds and models without a custom schema fall back to target.schema.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
