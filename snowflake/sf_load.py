"""
Snowflake Loader — GitHub Analytics Pipeline
=============================================
Loads processed local Parquet files into Snowflake raw table using the
Snowflake Python connector (PUT + COPY INTO).

Replaces bigquery/bq_load.py.

Usage:
    python snowflake/sf_load.py --date 2024-06-01
    python snowflake/sf_load.py --setup-only       # create tables only
    python snowflake/sf_load.py --dry-run          # validate paths, skip load
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

SF_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", "")        # e.g. xyz12345.us-east-1
SF_USER      = os.getenv("SNOWFLAKE_USER", "")
SF_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD", "")
SF_ROLE      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "GITHUB_PIPELINE")
SF_SCHEMA_RAW  = os.getenv("SNOWFLAKE_SCHEMA_RAW",  "RAW")
SF_SCHEMA_MART = os.getenv("SNOWFLAKE_SCHEMA_MART", "MART")

LOCAL_GCS_ROOT = os.getenv("LOCAL_GCS_ROOT", "./data/gcs-emulator")
GCS_BUCKET     = os.getenv("GCS_BUCKET", "github-analytics-raw")
SCHEMA_FILE    = Path(__file__).parent / "schema.sql"

RAW_TABLE = "RAW_GITHUB_EVENTS"

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>sf_load</cyan> | {message}",
    colorize=True,
)
logger.add("logs/sf_load.log", rotation="50 MB", retention="14 days")

os.makedirs("logs", exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────────────────────

def get_connection():
    import snowflake.connector
    if not SF_ACCOUNT or not SF_USER or not SF_PASSWORD:
        logger.error("SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD not set in .env")
        sys.exit(1)
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA_RAW,
    )
    logger.success(f"Connected to Snowflake: {SF_ACCOUNT} / {SF_DATABASE} ✓")
    return conn


def run_sql(cur, sql: str, label: str = ""):
    try:
        cur.execute(sql)
        logger.info(f"  ✓ {label or sql[:80].strip()}")
    except Exception as exc:
        if "already exists" in str(exc).lower():
            logger.info(f"  ⚙  Already exists: {label or sql[:60].strip()}")
        else:
            logger.warning(f"  ✗ {label}: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────────────

def setup(cur):
    """Create database, schemas, warehouse, and raw table if not present."""
    logger.info("Running setup ...")

    run_sql(cur, f"CREATE DATABASE IF NOT EXISTS {SF_DATABASE}", "Create database")
    run_sql(cur, f"USE DATABASE {SF_DATABASE}")
    run_sql(cur, f"CREATE SCHEMA IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA_RAW}",  "Create raw schema")
    run_sql(cur, f"CREATE SCHEMA IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA_MART}", "Create mart schema")
    run_sql(cur, f"CREATE WAREHOUSE IF NOT EXISTS {SF_WAREHOUSE} "
                 f"WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE", "Create warehouse")
    run_sql(cur, f"USE SCHEMA {SF_DATABASE}.{SF_SCHEMA_RAW}")

    if not SCHEMA_FILE.exists():
        logger.warning("snowflake/schema.sql not found — skipping DDL")
        return

    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    sql = sql.replace("{SF_DATABASE}", SF_DATABASE).replace("{SF_SCHEMA_RAW}", SF_SCHEMA_RAW)

    statements = []
    for raw_stmt in sql.split(";"):
        lines = []
        for line in raw_stmt.splitlines():
            stripped = line.strip()
            if stripped.startswith("--"):
                continue
            lines.append(line)
        cleaned = "\n".join(lines).strip()
        if cleaned:
            statements.append(cleaned)

    for stmt in statements:
        run_sql(cur, stmt)

    logger.success("Setup complete ✓")


# ─────────────────────────────────────────────────────────────────────────────
# Load
# ─────────────────────────────────────────────────────────────────────────────

def processed_local_path(date: str) -> Path:
    return Path(LOCAL_GCS_ROOT) / GCS_BUCKET / f"processed/date={date}"


def load_from_local(cur, date: str) -> int:
    """
    Upload local Parquet files to a Snowflake internal stage, then COPY INTO.
    Uses PUT (uploads file) + COPY INTO (loads into table).
    """
    import pandas as pd
    import snowflake.connector

    local_dir = processed_local_path(date)
    if not local_dir.exists():
        logger.error(f"Processed dir not found: {local_dir}")
        return 0

    files = list(local_dir.rglob("*.parquet"))
    if not files:
        logger.warning(f"No parquet files in {local_dir}")
        return 0

    logger.info(f"Loading {len(files)} parquet files for {date} ...")

    # Read and concatenate
    dfs = []
    for f in files:
        _df = pd.read_parquet(f)
        if "event_type" not in _df.columns:
            if "event_type=" in f.name:
                _df["event_type"] = f.name.split("event_type=")[1].split(".parquet")[0]
            elif "event_type=" in f.parent.name:
                _df["event_type"] = f.parent.name.split("event_type=")[1]
        dfs.append(_df)

    df  = pd.concat(dfs, ignore_index=True)
    df  = df.drop_duplicates(subset=["event_id"])

    # Coerce types
    for col in ["created_at", "ingested_at", "processed_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
            # Snowflake's COPY INTO from Parquet only reliably handles microsecond
            # precision. Nanosecond timestamps (datetime64[ns]) are silently loaded
            # as NULL, which causes all rows to be filtered out in dbt staging models.
            # Downcast to microseconds to ensure correct loading.
            df[col] = df[col].astype("datetime64[us, UTC]")

    for int_col in ["stars", "forks", "open_issues", "commit_count", "pr_number", "issue_number"]:
        if int_col in df.columns:
            # Int64 (nullable integer) saves as proper integer in Parquet,
            # preventing Snowflake from loading these columns as FLOAT/DECIMAL.
            df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype("Int64")

    for bool_col in ["is_tracked", "pr_merged", "is_merge", "has_text"]:
        if bool_col in df.columns:
            df[bool_col] = df[bool_col].fillna(False).astype(bool)

    if "topics" in df.columns:
        df["topics"] = df["topics"].apply(
            lambda v: str(v) if v is not None else "[]"
        )

    if "date_partition" not in df.columns:
        df["date_partition"] = pd.to_datetime(date).date()

    logger.info(f"  → {len(df):,} rows after dedup")

    # Write to temp parquet and PUT to Snowflake stage
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
        df.to_parquet(tmp_path, index=False, compression="snappy")

    stage_name = f"@{SF_DATABASE}.{SF_SCHEMA_RAW}.%{RAW_TABLE}"

    try:
        # Delete existing rows for this date partition so re-runs are idempotent
        # and bad rows from previous (NS-precision) loads are cleaned up.
        deleted = cur.execute(
            f"DELETE FROM {SF_DATABASE}.{SF_SCHEMA_RAW}.{RAW_TABLE} WHERE DATE_PARTITION = '{date}'"
        ).rowcount
        if deleted:
            logger.info(f"  Deleted {deleted:,} stale rows for date_partition={date}")

        logger.info(f"  PUT {tmp_path} → {stage_name}")
        # AUTO_COMPRESS=FALSE: avoid the .gz suffix mismatch in COPY INTO.
        # Snappy compression is already applied by to_parquet above.
        cur.execute(f"PUT file://{tmp_path} {stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        fname = os.path.basename(tmp_path)
        copy_sql = f"""
            COPY INTO {SF_DATABASE}.{SF_SCHEMA_RAW}.{RAW_TABLE}
            FROM {stage_name}
            PATTERN = '.*{fname}.*'
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
            ON_ERROR = 'ABORT_STATEMENT'
        """
        cur.execute(copy_sql)
        result = cur.fetchall()
        rows_loaded = sum(r[2] for r in result if r and len(r) > 2)
        logger.success(f"  ✓ {rows_loaded:,} rows loaded → {SF_DATABASE}.{SF_SCHEMA_RAW}.{RAW_TABLE}")
        return rows_loaded

    finally:
        os.unlink(tmp_path)


# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Load GitHub pipeline data to Snowflake")
    parser.add_argument(
        "--date",
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Date to load (YYYY-MM-DD), default: yesterday",
    )
    parser.add_argument("--setup-only", action="store_true", help="Create objects only, no load")
    parser.add_argument("--dry-run",    action="store_true", help="Validate paths, skip load")
    args = parser.parse_args()

    logger.info(f"Snowflake Loader — account={SF_ACCOUNT} db={SF_DATABASE}")
    logger.info(f"Date: {args.date}")

    if args.dry_run:
        path   = processed_local_path(args.date)
        exists = path.exists()
        logger.info(f"Dry run — local path: {path} → {'EXISTS ✓' if exists else 'NOT FOUND ✗'}")
        return

    conn = get_connection()
    cur  = conn.cursor()

    try:
        setup(cur)
        if args.setup_only:
            logger.success("Setup complete — exiting (--setup-only)")
            return
        load_from_local(cur, args.date)
        logger.success(f"Snowflake load complete for {args.date} ✓")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
