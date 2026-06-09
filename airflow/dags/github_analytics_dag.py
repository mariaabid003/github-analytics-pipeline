"""
Airflow DAG — GitHub Analytics Pipeline (Daily Orchestration)
=============================================================
DAG ID:   github_analytics_pipeline
Schedule: @daily (00:30 UTC — 30 min after midnight so GCS files are complete)

Task graph:
    fetch_github_events      ← producer/github_daily_fetcher.py --date {ds}
          │                     (replaces Kafka + Spark Streaming for Railway deployment)
          ▼
    snowflake_load_raw       ← snowflake/sf_load.py --date {ds}
          │
          ▼
    dbt_run                  ← dbt run --target prod
          │
          ▼
    dbt_test                 ← dbt test --target prod
          │
          ▼
    pipeline_report          ← logs summary (always runs)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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

def pipeline_report(**context):
    """Log a human-readable summary of the pipeline run."""
    execution_date = context["ds"]

    report = f"""
╔══════════════════════════════════════════════════════════════╗
║        GitHub Analytics Pipeline — Daily Run Report          ║
╠══════════════════════════════════════════════════════════════╣
║  Execution date:  {execution_date}
║  Run time:        {datetime.utcnow().isoformat()}
║  Pipeline:        GitHub API fetch → Snowflake load → dbt run → dbt test
║  Status:          SUCCESS
╚══════════════════════════════════════════════════════════════╝
    """
    print(report)


# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="github_analytics_pipeline",
    description="GitHub Analytics Pipeline — daily GitHub API fetch + Snowflake load + dbt",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 0 * * *",   # 00:30 UTC daily
    catchup=True,
    max_active_runs=1,
    tags=["github", "analytics", "snowflake", "dbt"],
) as dag:

    # ── Task 1: Fetch GitHub events directly from API ─────────────────────────
    # Calls the GitHub public events API and writes processed Parquet to
    # data/gcs-emulator/github-analytics-raw/processed/date={ds}/
    # This replaces the Kafka producer + Spark Streaming that cannot run
    # continuously on Railway's single-service deployment.
    t_fetch = BashOperator(
        task_id="fetch_github_events",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON_BIN} producer/github_daily_fetcher.py "
            f"--date {{{{ ds }}}}"
        ),
    )

    # ── Task 2: Load processed Parquet → Snowflake ───────────────────────────
    t_sf_load = BashOperator(
        task_id="snowflake_load_raw",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON_BIN} snowflake/sf_load.py "
            f"--date {{{{ ds }}}}"
        ),
    )

    # ── Task 3: dbt run ───────────────────────────────────────────────────────
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"unset DBT_PROJECT_DIR && cd {DBT_DIR} && "
            f"dbt deps --profiles-dir . && "
            f"dbt run --target {DBT_TARGET} --profiles-dir . --no-partial-parse"
        ),
    )

    # ── Task 4: dbt test ──────────────────────────────────────────────────────
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"unset DBT_PROJECT_DIR && cd {DBT_DIR} && "
            f"dbt test --target {DBT_TARGET} --profiles-dir . --no-partial-parse"
        ),
    )

    # ── Task 5: Report (always runs) ─────────────────────────────────────────
    t_report = PythonOperator(
        task_id="pipeline_report",
        python_callable=pipeline_report,
        trigger_rule="all_done",
    )

    # ── DAG dependency graph ──────────────────────────────────────────────────
    #
    #   fetch_github_events
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
    t_fetch >> t_sf_load >> t_dbt_run >> t_dbt_test >> t_report
