"""
BigQuery Loader — GitHub Analytics Pipeline
===========================================
Loads processed GCS Parquet files into BigQuery raw tables using the
BigQuery Python client (load_table_from_uri for GCS, load_table_from_dataframe
for local/emulator mode).

Usage:
    python bigquery/bq_load.py --date 2024-06-01
    python bigquery/bq_load.py --setup-only       # create tables only
    python bigquery/bq_load.py --dry-run          # validate paths, skip load
"""

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

PROJECT_ID      = os.getenv("GCP_PROJECT_ID", "")
DATASET_RAW     = os.getenv("BQ_DATASET_RAW", "github_raw")
GCS_BUCKET      = os.getenv("GCS_BUCKET", "github-analytics-raw")
EMULATOR_HOST   = os.getenv("GCS_EMULATOR_HOST", "")
LOCAL_GCS_ROOT  = os.getenv("LOCAL_GCS_ROOT", "./data/gcs-emulator")
SCHEMA_FILE     = Path(__file__).parent / "schema.sql"

IS_LOCAL = bool(EMULATOR_HOST)

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>bq_load</cyan> | {message}",
    colorize=True,
)
logger.add("logs/bq_load.log", rotation="50 MB", retention="14 days")

os.makedirs("logs", exist_ok=True)

RAW_TABLE = "raw_github_events"


# ─────────────────────────────────────────────────────────────────────────────
# BigQuery client helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_client():
    from google.cloud import bigquery
    if not PROJECT_ID:
        logger.error("GCP_PROJECT_ID not set in .env")
        sys.exit(1)
    return bigquery.Client(project=PROJECT_ID)


def ensure_dataset(client):
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    ref = client.dataset(DATASET_RAW)
    try:
        client.get_dataset(ref)
        logger.info(f"Dataset {DATASET_RAW} exists ✓")
    except NotFound:
        ds = bigquery.Dataset(ref)
        ds.location = os.getenv("GCP_LOCATION", "US")
        ds.description = "GitHub Analytics Pipeline — raw events from GCS"
        client.create_dataset(ds)
        logger.success(f"Created dataset {PROJECT_ID}.{DATASET_RAW}")


def ensure_tables(client):
    if not SCHEMA_FILE.exists():
        logger.warning("schema.sql not found — skipping DDL")
        return

    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    sql = sql.replace("`github_raw.", f"`{PROJECT_ID}.{DATASET_RAW}.")

    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if not stmt or stmt.startswith("--"):
            continue
        try:
            client.query(stmt).result()
            logger.info(f"  ✓ {stmt[:80].strip()}")
        except Exception as exc:
            if "Already Exists" in str(exc):
                logger.info("  ⚙  Table already exists")
            else:
                logger.warning(f"  ✗ DDL error: {exc}")

    logger.success("Table setup complete ✓")


# ─────────────────────────────────────────────────────────────────────────────
# Load helpers
# ─────────────────────────────────────────────────────────────────────────────

def processed_local_path(date: str) -> Path:
    return Path(LOCAL_GCS_ROOT) / GCS_BUCKET / f"processed/date={date}"


def processed_gcs_uri(date: str) -> str:
    return f"gs://{GCS_BUCKET}/processed/date={date}/*.parquet"


def load_from_local(client, date: str) -> int:
    """Load from local parquet files (emulator mode) via load_table_from_dataframe."""
    import pandas as pd
    from google.cloud import bigquery

    local_dir = processed_local_path(date)
    if not local_dir.exists():
        logger.error(f"Processed dir not found: {local_dir}")
        return 0

    files = list(local_dir.rglob("*.parquet"))
    if not files:
        logger.warning(f"No parquet files in {local_dir}")
        return 0

    logger.info(f"Loading {len(files)} local parquet files for {date} ...")
    dfs = [pd.read_parquet(f) for f in files]
    df  = pd.concat(dfs, ignore_index=True)
    logger.info(f"  → {len(df):,} rows loaded")

    # Coerce timestamps
    for col in ["created_at", "ingested_at", "processed_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    if "date_partition" not in df.columns:
        df["date_partition"] = pd.to_datetime(date).date()
    else:
        df["date_partition"] = pd.to_datetime(df["date_partition"]).dt.date

    # Dedup
    df = df.drop_duplicates(subset=["event_id"])

    table_ref  = f"{PROJECT_ID}.{DATASET_RAW}.{RAW_TABLE}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
    logger.success(f"  ✓ {len(df):,} rows → {table_ref}")
    return len(df)


def load_from_gcs(client, date: str) -> int:
    """Load directly from GCS URI (production mode)."""
    from google.cloud import bigquery

    uri        = processed_gcs_uri(date)
    table_ref  = f"{PROJECT_ID}.{DATASET_RAW}.{RAW_TABLE}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )

    logger.info(f"Loading from GCS: {uri}")
    job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    job.result()
    table = client.get_table(table_ref)
    logger.success(f"  ✓ Loaded {job.output_rows:,} rows → {table_ref}")
    return job.output_rows


# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Load GitHub pipeline data to BigQuery")
    parser.add_argument(
        "--date",
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Date to load (YYYY-MM-DD), default: yesterday",
    )
    parser.add_argument("--setup-only", action="store_true", help="Create tables only, no load")
    parser.add_argument("--dry-run",    action="store_true", help="Validate paths, skip load")
    args = parser.parse_args()

    logger.info(f"BigQuery Loader — project={PROJECT_ID} raw_dataset={DATASET_RAW}")
    logger.info(f"Date: {args.date} | Mode: {'local' if IS_LOCAL else 'GCS'}")

    if args.dry_run:
        if IS_LOCAL:
            path = processed_local_path(args.date)
            exists = path.exists()
            logger.info(f"Dry run — local path: {path} → {'EXISTS ✓' if exists else 'NOT FOUND ✗'}")
        else:
            logger.info(f"Dry run — GCS URI: {processed_gcs_uri(args.date)}")
        return

    client = get_client()
    ensure_dataset(client)
    ensure_tables(client)

    if args.setup_only:
        logger.success("Setup complete — exiting (--setup-only)")
        return

    if IS_LOCAL:
        load_from_local(client, args.date)
    else:
        load_from_gcs(client, args.date)

    logger.success(f"BigQuery load complete for {args.date} ✓")


if __name__ == "__main__":
    main()
