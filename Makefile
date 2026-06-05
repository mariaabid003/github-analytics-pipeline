# ─────────────────────────────────────────────────────────────────────────────
# GitHub Analytics Pipeline — Makefile
# Usage: make <target>
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: help install up down restart status logs \
        producer streaming batch sf-setup sf-load \
        dbt-deps dbt-run dbt-test dbt-all powerbi-export clean

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  GitHub Analytics Pipeline — Commands"
	@echo "  ─────────────────────────────────────────────────────────────"
	@echo "  INFRASTRUCTURE"
	@echo "    make install      Install Python dependencies"
	@echo "    make up           Start all Docker services"
	@echo "    make down         Stop all Docker services"
	@echo "    make status       Show Docker container status"
	@echo ""
	@echo "  PIPELINE COMPONENTS (run locally)"
	@echo "    make producer     GitHub Events producer (Kafka)"
	@echo "    make streaming    Spark Structured Streaming (Kafka → GCS/local)"
	@echo "    make batch        Spark Batch Processor (GCS raw → GCS processed)"
	@echo ""
	@echo "  SNOWFLAKE"
	@echo "    make sf-setup     Create Snowflake database, schemas + tables"
	@echo "    make sf-load      Load yesterday's data into Snowflake"
	@echo ""
	@echo "  DBT"
	@echo "    make dbt-deps     Install dbt packages (dbt-utils)"
	@echo "    make dbt-run      Run all dbt models (dev target = DuckDB)"
	@echo "    make dbt-test     Run all dbt tests"
	@echo "    make dbt-all      deps + run + test"
	@echo ""
	@echo "  DASHBOARD (Power BI)"
	@echo "    make powerbi-export   Export mart tables to CSV for Power BI Desktop"
	@echo "    make powerbi-demo     Export demo data (no Snowflake required)"
	@echo ""
	@echo "  UTILITIES"
	@echo "    make clean        Remove generated data/parquet files"
	@echo "    make logs         Tail all log files"
	@echo ""

# ── Python ────────────────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

# ── Docker ────────────────────────────────────────────────────────────────────
up:
	docker compose up -d --build
	@echo ""
	@echo "  Services starting — wait ~90s for full health"
	@echo "  Airflow UI  → http://localhost:8080  (admin / admin)"
	@echo "  Kafka       → localhost:9092"
	@echo "  GCS (local) → http://localhost:4443"
	@echo ""

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

status:
	docker compose ps

# ── Pipeline Components ───────────────────────────────────────────────────────
producer:
	@echo "Starting GitHub Events producer (Ctrl+C to stop)..."
	python producer/github_producer.py

streaming:
	@echo "Starting Spark Structured Streaming → GCS (--no-spark mode, Ctrl+C to stop)..."
	python spark_streaming/github_to_gcs.py --no-spark

batch:
	@echo "Running Spark Batch Processor for yesterday..."
	python spark_batch/github_batch_processor.py --no-spark

batch-date:
	@echo "Usage: make batch-date DATE=2024-06-01"
	python spark_batch/github_batch_processor.py --date $(DATE) --no-spark

# ── Snowflake ─────────────────────────────────────────────────────────────────
sf-setup:
	python snowflake/sf_load.py --setup-only

sf-load:
	python snowflake/sf_load.py

sf-dry-run:
	python snowflake/sf_load.py --dry-run

# ── dbt ───────────────────────────────────────────────────────────────────────
dbt-deps:
	cd dbt && dbt deps --profiles-dir .

dbt-run:
	cd dbt && dbt run --profiles-dir . --target $(or $(DBT_TARGET),dev)

dbt-test:
	cd dbt && dbt test --profiles-dir . --target $(or $(DBT_TARGET),dev)

dbt-all: dbt-deps dbt-run dbt-test

dbt-prod:
	cd dbt && dbt run --profiles-dir . --target prod && dbt test --profiles-dir . --target prod

dbt-docs:
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# ── Dashboard (Power BI) ──────────────────────────────────────────────────────
powerbi-export:
	@echo "Exporting Snowflake mart tables to dashboard/exports/ for Power BI..."
	python dashboard/export_for_powerbi.py

powerbi-demo:
	@echo "Exporting demo data to dashboard/exports/ for Power BI..."
	python dashboard/export_for_powerbi.py --demo

# ── Utilities ─────────────────────────────────────────────────────────────────
logs:
	@powershell -Command "Get-ChildItem logs\*.log | ForEach-Object { Get-Content $$_.FullName -Wait -Tail 50 }" 2>/dev/null || echo "No logs yet — run 'make producer' first"

clean:
	@powershell -Command "if (Test-Path data\gcs-emulator) { Remove-Item -Recurse -Force data\gcs-emulator }"
	@powershell -Command "if (Test-Path data\checkpoints)  { Remove-Item -Recurse -Force data\checkpoints  }"
	@powershell -Command "if (Test-Path dbt\target)        { Remove-Item -Recurse -Force dbt\target        }"
	@powershell -Command "if (Test-Path dbt\dev.duckdb)    { Remove-Item -Force dbt\dev.duckdb            }"
	@echo "Clean complete."
