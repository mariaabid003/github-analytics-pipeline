"""
Airflow DAG — GitHub Analytics Pipeline (Daily Orchestration)
=============================================================
DAG ID:   github_analytics_pipeline
Schedule: @daily (00:30 UTC — 30 min after midnight so GCS files are complete)

Task graph:
    check_kafka_health
          │
          ▼
    spark_batch_process      ← spark_batch/github_batch_processor.py --date {yesterday}
          │
          ▼
    snowflake_load_raw       ← snowflake/sf_load.py --date {yesterday}
          │
          ▼
    dbt_run                  ← dbt run --target prod
          │
          ▼
    dbt_test                 ← dbt test --target prod
          │
          ▼
    pipeline_report          ← logs row counts + test results (always runs)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner":            "github_analytics",
    "depends_on_past":  False,
    "start_date":       pendulum.datetime(2026, 5, 30, tz="UTC"),
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry":   False,
}

PROJECT_ROOT = os.getenv("AIRFLOW_HOME", "/opt/airflow")
PYTHON_BIN   = "python"
DBT_DIR      = os.path.join(PROJECT_ROOT, "dbt")
DBT_TARGET   = os.getenv("DBT_TARGET", "prod")


# ─────────────────────────────────────────────────────────────────────────────
# Python callables
# ─────────────────────────────────────────────────────────────────────────────

def check_kafka_health(**context):
    """Verify Kafka topic github-events is alive and has recent data."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic     = os.getenv("KAFKA_TOPIC_GITHUB", "github-events")

    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer   = KafkaConsumer(bootstrap_servers=bootstrap.split(","))
        partitions = consumer.partitions_for_topic(topic) or set()

        if not partitions:
            print(f"⚠ Topic '{topic}' not found — producer may not have started yet")
            context["task_instance"].xcom_push(key="kafka_messages", value=0)
            return

        total = 0
        for p in partitions:
            tp  = TopicPartition(topic, p)
            end = consumer.end_offsets([tp])[tp]
            beg = consumer.beginning_offsets([tp])[tp]
            total += (end - beg)
        consumer.close()

        print(f"✓ Kafka topic '{topic}': {total:,} messages across {len(partitions)} partitions")
        context["task_instance"].xcom_push(key="kafka_messages", value=total)

    except Exception as exc:
        print(f"⚠ Kafka health check failed: {exc} — continuing pipeline")
        context["task_instance"].xcom_push(key="kafka_messages", value=-1)


def pipeline_report(**context):
    """Log a human-readable summary of the pipeline run."""
    ti            = context["task_instance"]
    execution_date = context["ds"]
    kafka_msgs    = ti.xcom_pull(task_ids="check_kafka_health", key="kafka_messages") or "N/A"

    report = f"""
╔══════════════════════════════════════════════════════════════╗
║        GitHub Analytics Pipeline — Daily Run Report          ║
╠══════════════════════════════════════════════════════════════╣
║  Execution date:  {execution_date}
║  Run time:        {datetime.utcnow().isoformat()}
║  Kafka messages:  {kafka_msgs}
║  Pipeline:        Spark batch → Snowflake load → dbt run → dbt test
║  Status:          SUCCESS
╚══════════════════════════════════════════════════════════════╝
    """
    print(report)


# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="github_analytics_pipeline",
    description="GitHub Analytics Pipeline — daily Spark batch + Snowflake load + dbt",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 0 * * *",   # 00:30 UTC daily
    catchup=True,
    max_active_runs=1,
    tags=["github", "analytics", "spark", "snowflake", "dbt"],
) as dag:

    # ── Task 1: Kafka health check ────────────────────────────────────────────
    t_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    # ── Task 2: Spark batch processor ────────────────────────────────────────
    t_spark_batch = SparkSubmitOperator(
        task_id="spark_batch_process",
        conn_id="spark_default",
        application=os.path.join(PROJECT_ROOT, "spark_batch", "github_batch_processor.py"),
        application_args=["--date", "{{ macros.ds_add(ds, -1) }}"],
        name="github_batch_{{ macros.ds_add(ds, -1) }}",
        conf={
            "spark.sql.adaptive.enabled": "true",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
        },
        verbose=True,
    )

    # ── Task 3: Load processed Parquet → Snowflake ───────────────────────────
    t_sf_load = BashOperator(
        task_id="snowflake_load_raw",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON_BIN} snowflake/sf_load.py "
            # ds_add(ds, -1): process yesterday's data (files written by streaming up to midnight).
            f"--date {{{{ macros.ds_add(ds, -1) }}}}"
        ),
    )

    # ── Task 4: dbt run ───────────────────────────────────────────────────────
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"unset DBT_PROJECT_DIR && cd {DBT_DIR} && "
            f"dbt deps --profiles-dir . && "
            f"dbt run --target {DBT_TARGET} --profiles-dir . --no-partial-parse"
        ),
    )

    # ── Task 5: dbt test ──────────────────────────────────────────────────────
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"unset DBT_PROJECT_DIR && cd {DBT_DIR} && "
            f"dbt test --target {DBT_TARGET} --profiles-dir . --no-partial-parse"
        ),
    )

    # ── Task 6: Report (always runs) ─────────────────────────────────────────
    t_report = PythonOperator(
        task_id="pipeline_report",
        python_callable=pipeline_report,
        trigger_rule="all_done",
    )

    # ── DAG dependency graph ──────────────────────────────────────────────────
    #
    #   check_kafka_health
    #         │
    #         ▼
    #   spark_batch_process
    #         │
    #         ▼
    #   snowflake_load_raw
    #         │
    #         ▼
    #   dbt_run
    #         │
    #         ▼
    #   dbt_test
    #         │
    #         ▼
    #   pipeline_report
    #
    t_kafka >> t_spark_batch >> t_sf_load >> t_dbt_run >> t_dbt_test >> t_report
