"""
Spark Batch Processor — GCS Raw → GCS Processed
=================================================
Daily batch job (triggered by Airflow) that reads yesterday's raw Parquet
from GCS, deduplicates, enriches, and writes to the processed GCS path.

Input:  gs://{bucket}/raw/date={date}/
Output: gs://{bucket}/processed/date={date}/

Derived fields added:
    is_merge        bool    — PR event with pr_merged=True
    has_text        bool    — text_content non-empty
    pr_state        string  — open | closed | merged
    repo_age_days   int     — approx days since first seen in dataset (approx)

Modes:
    Spark mode  (default) — requires Java 8/11
    Python mode (--no-spark) — pure pandas/pyarrow

Usage:
    python spark_batch/github_batch_processor.py --date 2024-06-01
    python spark_batch/github_batch_processor.py --date 2024-06-01 --no-spark
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

GCS_BUCKET      = os.getenv("GCS_BUCKET", "github-analytics-raw")
EMULATOR_HOST   = os.getenv("GCS_EMULATOR_HOST", "")
LOCAL_GCS_ROOT  = os.getenv("LOCAL_GCS_ROOT", "./data/gcs-emulator")
CHECKPOINT_DIR  = "./data/checkpoints/github_batch"

IS_LOCAL = bool(EMULATOR_HOST)

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>batch</cyan> | {message}",
    colorize=True,
)
os.makedirs("logs", exist_ok=True)
logger.add("logs/github_batch.log", rotation="100 MB", retention="14 days", level="DEBUG")


# ─────────────────────────────────────────────────────────────────────────────
# Path helpers
# ─────────────────────────────────────────────────────────────────────────────

def raw_path(date: str) -> str:
    base = f"raw/date={date}"
    if IS_LOCAL:
        return str(Path(LOCAL_GCS_ROOT) / GCS_BUCKET / base)
    return f"gs://{GCS_BUCKET}/{base}"


def processed_path(date: str) -> str:
    base = f"processed/date={date}"
    if IS_LOCAL:
        return str(Path(LOCAL_GCS_ROOT) / GCS_BUCKET / base)
    return f"gs://{GCS_BUCKET}/{base}"


# ─────────────────────────────────────────────────────────────────────────────
# Enrichment logic (shared between Spark and Python modes)
# ─────────────────────────────────────────────────────────────────────────────

def enrich_dataframe(df):
    """Add derived columns to a pandas DataFrame of raw events."""
    import pandas as pd

    df = df.copy()

    # Dedup
    before = len(df)
    df = df.drop_duplicates(subset=["event_id"])
    logger.info(f"Deduplication: {before} → {len(df)} rows (removed {before - len(df)} dups)")

    # Derived booleans
    df["is_merge"] = (
        (df.get("event_type", "") == "PullRequestEvent") &
        (df.get("pr_merged", False).fillna(False).astype(bool))
    )
    df["has_text"] = df.get("text_content", "").fillna("").str.len() > 0

    # PR state
    def pr_state(row):
        if row.get("event_type") != "PullRequestEvent":
            return ""
        if row.get("pr_merged", False):
            return "merged"
        action = row.get("action", "")
        if action == "closed":
            return "closed"
        return "open"

    df["pr_state"] = df.apply(pr_state, axis=1)

    # Normalise timestamps
    for col in ["created_at", "ingested_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # Add processed_at
    df["processed_at"] = datetime.now(timezone.utc)

    # Safe type coercions
    for int_col in ["stars", "forks", "open_issues", "commit_count", "pr_number", "issue_number"]:
        if int_col in df.columns:
            # Int64 (nullable integer) saves as proper integer in Parquet,
            # preventing Snowflake from loading these columns as FLOAT/DECIMAL.
            df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype("Int64")

    for bool_col in ["is_tracked", "pr_merged", "is_merge", "has_text"]:
        if bool_col in df.columns:
            df[bool_col] = df[bool_col].fillna(False).astype(bool)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Python-only batch processor
# ─────────────────────────────────────────────────────────────────────────────

def run_python_batch(date: str):
    """Read raw Parquet for `date`, enrich, write processed Parquet."""
    import pandas as pd

    in_path  = raw_path(date)
    out_path = processed_path(date)

    logger.info(f"Input  path: {in_path}")
    logger.info(f"Output path: {out_path}")

    # Find all parquet files in the date partition
    in_dir = Path(in_path)
    if not in_dir.exists():
        logger.error(f"Raw partition not found: {in_dir}")
        sys.exit(1)

    parquet_files = list(in_dir.rglob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No parquet files found in {in_dir} — nothing to process")
        return

    logger.info(f"Found {len(parquet_files)} parquet files")
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df  = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded {len(df):,} raw rows")

    df = enrich_dataframe(df)
    logger.info(f"Enriched: {len(df):,} rows after dedup")

    # Write output, partitioned by event_type
    os.makedirs(out_path, exist_ok=True)
    for event_type, group in df.groupby("event_type"):
        safe_type = str(event_type).replace("/", "_")
        out_file  = os.path.join(out_path, f"event_type={safe_type}.parquet")
        group.drop(columns=["event_type"], errors="ignore").to_parquet(
            out_file, index=False, compression="snappy"
        )
        logger.success(f"  ✓ {len(group):>5} rows → event_type={safe_type}")

    logger.success(f"Batch complete: {len(df):,} rows written to {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Spark batch processor
# ─────────────────────────────────────────────────────────────────────────────

def run_spark_batch(date: str):
    """Spark batch version — better for large volumes (millions of events)."""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, lit, when, length, current_timestamp,
        to_timestamp, coalesce,
    )

    logger.info("Initialising Spark session for batch ...")

    builder = (
        SparkSession.builder
        .appName(f"GitHubBatchProcessor_{date}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
    )

    if not IS_LOCAL:
        builder = builder.config(
            "spark.jars.packages",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.18"
        ).config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        ).config(
            "spark.hadoop.google.cloud.auth.service.account.enable", "true"
        ).config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.success("Spark session started ✓")

    in_path  = raw_path(date)
    out_path = processed_path(date)

    logger.info(f"Reading raw data from: {in_path}")
    df = spark.read.parquet(in_path)
    before = df.count()
    logger.info(f"Loaded {before:,} raw rows")

    # Dedup
    df = df.dropDuplicates(["event_id"])
    logger.info(f"After dedup: {df.count():,} rows")

    # Derived columns
    df = (
        df
        .withColumn("is_merge",
            (col("event_type") == "PullRequestEvent") & (col("pr_merged") == True))
        .withColumn("has_text",
            col("text_content").isNotNull() & (length(col("text_content")) > 0))
        .withColumn("pr_state",
            when((col("event_type") == "PullRequestEvent") & (col("pr_merged") == True), lit("merged"))
            .when((col("event_type") == "PullRequestEvent") & (col("action") == "closed"), lit("closed"))
            .when(col("event_type") == "PullRequestEvent", lit("open"))
            .otherwise(lit("")))
        .withColumn("processed_at", current_timestamp())
    )

    logger.info(f"Writing processed data to: {out_path}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("event_type")
        .parquet(out_path)
    )
    logger.success(f"Spark batch complete → {out_path}")


# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="GitHub Batch Processor")
    parser.add_argument(
        "--date",
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Date to process (YYYY-MM-DD), default: yesterday",
    )
    parser.add_argument("--no-spark", action="store_true", help="Use Python/pandas instead of Spark")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logger.info(f"Processing date: {args.date}")
    logger.info(f"Mode: {'Python/pandas' if args.no_spark else 'Spark'}")
    logger.info(f"Storage: {'local emulator' if IS_LOCAL else f'gs://{GCS_BUCKET}'}")

    if args.no_spark:
        run_python_batch(args.date)
    else:
        try:
            run_spark_batch(args.date)
        except Exception as e:
            logger.error(f"Spark failed ({e}). Falling back to Python mode.")
            run_python_batch(args.date)
