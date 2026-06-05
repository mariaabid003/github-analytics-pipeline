"""
Spark Structured Streaming — GitHub Events → GCS
=================================================
Reads the `github-events` Kafka topic and writes raw Parquet files to GCS
(or to the local filesystem when using fake-gcs-server in dev mode).

Partition layout:
    gs://{bucket}/raw/date=YYYY-MM-DD/hour=HH/event_type={type}/batch_{ts}.parquet

Modes:
    Spark mode  (default) — requires Java 8/11
    Python mode (--no-spark) — pure kafka-python fallback, no Java needed

Usage:
    python spark_streaming/github_to_gcs.py
    python spark_streaming/github_to_gcs.py --no-spark

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
    KAFKA_TOPIC_GITHUB        default: github-events
    GCS_BUCKET                GCS bucket name
    GCS_EMULATOR_HOST         if set, writes to LOCAL_GCS_ROOT instead of real GCS
    LOCAL_GCS_ROOT            local path for emulator (default: ./data/gcs-emulator)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC_GITHUB", "github-events")
GCS_BUCKET      = os.getenv("GCS_BUCKET", "github-analytics-raw")
EMULATOR_HOST   = os.getenv("GCS_EMULATOR_HOST", "")
LOCAL_GCS_ROOT  = os.getenv("LOCAL_GCS_ROOT", "./data/gcs-emulator")
CHECKPOINT_DIR  = "./data/checkpoints/github_streaming"
BATCH_INTERVAL  = "30 seconds"
BATCH_SIZE      = 200   # Python fallback: flush every N messages

IS_LOCAL = bool(EMULATOR_HOST)

# ── Logging ───────────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>streaming</cyan> | {message}",
    colorize=True,
)
os.makedirs("logs", exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
logger.add("logs/github_to_gcs.log", rotation="100 MB", retention="7 days", level="DEBUG")


# ─────────────────────────────────────────────────────────────────────────────
# Path helpers
# ─────────────────────────────────────────────────────────────────────────────

def gcs_path(date: str, hour: str, event_type: str) -> str:
    """Return the output path for a partition (GCS URI or local path)."""
    partition = f"date={date}/hour={hour}/event_type={event_type}"
    if IS_LOCAL:
        return str(Path(LOCAL_GCS_ROOT) / GCS_BUCKET / "raw" / partition)
    return f"gs://{GCS_BUCKET}/raw/{partition}"


def write_parquet_batch(records: list[dict]) -> int:
    """Write a list of records to Parquet, partitioned by date/hour/event_type."""
    import pandas as pd

    if not records:
        return 0

    df = pd.DataFrame(records)

    # Ensure timestamp columns exist
    if "created_at" not in df.columns:
        df["created_at"] = datetime.now(timezone.utc).isoformat()

    # Parse date/hour partitions
    df["_dt"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce").fillna(
        pd.Timestamp.now(tz="UTC")
    )
    df["_date"]  = df["_dt"].dt.strftime("%Y-%m-%d")
    df["_hour"]  = df["_dt"].dt.strftime("%H")
    df["_etype"] = df.get("event_type", "Unknown").fillna("Unknown")

    total = 0
    for (date, hour, etype), group in df.groupby(["_date", "_hour", "_etype"]):
        out_dir = gcs_path(date, hour, etype)
        os.makedirs(out_dir, exist_ok=True)
        ts      = int(time.time() * 1000)
        out_path = os.path.join(out_dir, f"batch_{ts}.parquet")
        group.drop(columns=["_dt", "_date", "_hour", "_etype"], errors="ignore").to_parquet(
            out_path, index=False, compression="snappy"
        )
        total += len(group)
        logger.debug(f"Wrote {len(group)} rows → {out_path}")

    return total


# ─────────────────────────────────────────────────────────────────────────────
# Spark Structured Streaming mode
# ─────────────────────────────────────────────────────────────────────────────

def run_spark_streaming():
    """Read from Kafka and write to GCS via Spark Structured Streaming."""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, to_date, hour as spark_hour
    from pyspark.sql.types import (
        ArrayType, BooleanType, IntegerType, StringType, StructField, StructType,
    )

    logger.info("Initialising Spark session ...")

    builder = (
        SparkSession.builder
        .appName("GitHubEventsToGCS")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )

    if IS_LOCAL:
        # No GCS connector needed — writing to local filesystem
        logger.info("Dev mode: writing to local filesystem (GCS emulator path)")
    else:
        # Wire up the GCS connector for real GCS
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
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

    schema = StructType([
        StructField("event_id",      StringType()),
        StructField("event_type",    StringType()),
        StructField("action",        StringType()),
        StructField("actor",         StringType()),
        StructField("org",           StringType()),
        StructField("repo_name",     StringType()),
        StructField("repo_owner",    StringType()),
        StructField("language",      StringType()),
        StructField("stars",         IntegerType()),
        StructField("forks",         IntegerType()),
        StructField("open_issues",   IntegerType()),
        StructField("description",   StringType()),
        StructField("topics",        ArrayType(StringType())),
        StructField("text_content",  StringType()),
        StructField("commit_count",  IntegerType()),
        StructField("pr_merged",     BooleanType()),
        StructField("pr_number",     IntegerType()),
        StructField("issue_number",  IntegerType()),
        StructField("ref_type",      StringType()),
        StructField("ref_name",      StringType()),
        StructField("is_tracked",    BooleanType()),
        StructField("created_at",    StringType()),
        StructField("ingested_at",   StringType()),
    ])

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), schema).alias("d"))
        .select("d.*")
    )

    from pyspark.sql.functions import coalesce, current_timestamp, date_format, to_timestamp, lit

    # Drop rows with null event_type — they can't be partitioned or classified,
    # and stg_github_events.sql filters them out anyway. Keeping them would create
    # a spurious event_type=Unknown partition and pollute downstream models.
    parsed = parsed.filter(col("event_type").isNotNull())

    # Add partition columns directly via Spark SQL functions
    parsed = parsed.withColumn("_dt", coalesce(to_timestamp(col("created_at")), current_timestamp())) \
                   .withColumn("date", date_format(col("_dt"), "yyyy-MM-dd")) \
                   .withColumn("hour", date_format(col("_dt"), "HH"))

    output_path = f"gs://{GCS_BUCKET}/raw" if not IS_LOCAL else str(Path(LOCAL_GCS_ROOT) / GCS_BUCKET / "raw")

    query = (
        parsed.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .partitionBy("date", "hour", "event_type")
        .trigger(processingTime=BATCH_INTERVAL)
        .start()
    )

    logger.success(f"Streaming started — consuming '{KAFKA_TOPIC}' ...")
    query.awaitTermination()


# ─────────────────────────────────────────────────────────────────────────────
# Python-only fallback (no Java / Spark required)
# ─────────────────────────────────────────────────────────────────────────────

def run_python_consumer():
    """Pure kafka-python consumer that batches events and writes Parquet."""
    from kafka import KafkaConsumer

    logger.warning("Python-only mode — no Spark/Java needed. Use Spark for production.")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS.split(","),
        auto_offset_reset="latest",
        group_id="github-to-gcs-python",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=-1,
    )

    logger.info(f"Listening on '{KAFKA_TOPIC}' → {'local emulator' if IS_LOCAL else f'gs://{GCS_BUCKET}'}")

    batch: list[dict] = []
    stats = {"written": 0, "batches": 0, "errors": 0}

    for msg in consumer:
        try:
            batch.append(msg.value)

            if len(batch) >= BATCH_SIZE:
                n = write_parquet_batch(batch)
                stats["written"] += n
                stats["batches"] += 1
                logger.info(
                    f"📦 Flushed batch {stats['batches']}: "
                    f"{n} rows | total written: {stats['written']:,}"
                )
                batch = []

        except Exception as exc:
            stats["errors"] += 1
            logger.warning(f"Consumer error: {exc}")
            time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    use_spark = "--no-spark" not in sys.argv

    if use_spark:
        logger.info("Starting Spark Structured Streaming ...")
        try:
            run_spark_streaming()
        except Exception as e:
            logger.error(f"Spark failed ({e}). Falling back to Python consumer.")
            run_python_consumer()
    else:
        logger.info("Starting Python consumer (--no-spark) ...")
        run_python_consumer()
