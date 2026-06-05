# 🐙 GitHub Analytics Pipeline

> **Data Engineering Portfolio Project** — End-to-end streaming + batch pipeline
> ingesting GitHub public events, processing through Spark, storing in GCS,
> loading to BigQuery, and transforming with dbt into analytics-ready mart tables.

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.6-231F20?logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.9-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt&logoColor=white)](https://getdbt.com)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-4285F4?logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Power BI](https://img.shields.io/badge/Power_BI-Desktop-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com)

---

## 🏗️ Architecture

```
GitHub Public Events API
         │  (poll every 5s via PAT)
         ▼
 producer/github_producer.py
         │  publishes JSON to Kafka
         ▼
 Kafka topic: github-events
         │
         ├──► spark_streaming/github_to_gcs.py    (micro-batch every 30s)
         │         │  Parquet partitioned by date/hour/event_type
         │         ▼
         │    GCS: gs://github-analytics-raw/raw/date=YYYY-MM-DD/
         │
         └──► (Airflow @daily at 00:30 UTC)
                   │
                   ▼
         spark_batch/github_batch_processor.py
                   │  deduplicate · enrich · derive fields
                   ▼
         GCS: gs://github-analytics-raw/processed/date=YYYY-MM-DD/
                   │
                   ▼
         bigquery/bq_load.py            (bq load from GCS URI)
                   │
                   ▼
         BigQuery: github_raw.raw_github_events  (partitioned + clustered)
                   │
                   ▼
         dbt run (staging → mart)
                   │
           ┌───────┴───────┐
           ▼               ▼
   fact_commits       fact_prs
   dim_repos          dim_contributors
                   │
                   ▼
         Power BI Desktop   (direct Snowflake connector)
         OR dashboard/export_for_powerbi.py → CSV → Power BI
```

---

## 📁 Project Structure

```
github-analytics-pipeline/
├── docker-compose.yml              # Kafka + Zookeeper + fake-gcs + Airflow
├── .env.example                    # Credential template
├── requirements.txt                # Python dependencies
├── Makefile                        # One-command shortcuts
│
├── producer/
│   └── github_producer.py          # GitHub Events API → Kafka
│
├── spark_streaming/
│   └── github_to_gcs.py            # Kafka → GCS raw Parquet (Spark or Python)
│
├── spark_batch/
│   └── github_batch_processor.py   # GCS raw → GCS processed (Spark or Python)
│
├── bigquery/
│   ├── schema.sql                  # DDL for raw_github_events
│   └── bq_load.py                  # GCS processed → BigQuery
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                # dev=DuckDB  prod=BigQuery
│   ├── packages.yml
│   └── models/
│       ├── staging/
│       │   ├── _sources.yml
│       │   ├── stg_github_events.sql
│       │   ├── stg_push_events.sql
│       │   ├── stg_pr_events.sql
│       │   └── stg_watch_events.sql
│       └── mart/
│           ├── _schema.yml         # Tests + docs
│           ├── fact_commits.sql
│           ├── fact_prs.sql
│           ├── dim_repos.sql
│           └── dim_contributors.sql
│
├── airflow/
│   └── dags/
│       └── github_analytics_dag.py # Daily: Spark → BQ load → dbt run → dbt test
│
└── dashboard/
    └── export_for_powerbi.py       # Exports mart tables to CSV for Power BI Desktop
```

---

## 🚀 Quick Start

### Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Docker Desktop | Latest | [docker.com](https://docker.com) |
| Python | 3.11+ | [python.org](https://python.org) |
| Java (JDK) | 8 or 11 | *Optional* — only for Spark mode. Use `--no-spark` to skip. |

### Step 1 — Configure credentials

```powershell
# Copy template and fill in your values
copy .env.example .env
```

Minimum required in `.env`:
```env
GITHUB_TOKEN=ghp_yourtoken       # https://github.com/settings/tokens
GCS_EMULATOR_HOST=localhost:4443  # keep this for local dev
```

### Step 2 — Start infrastructure

```powershell
make up
# Wait ~90 seconds for all services to become healthy
make status
```

Services started:
- **Kafka** `localhost:9092` — event streaming
- **fake-gcs-server** `localhost:4443` — local GCS emulator (no real GCP needed)
- **Airflow** `http://localhost:8080` — orchestration UI (admin/admin)

### Step 3 — Install Python dependencies

```powershell
make install
```

### Step 4 — Run the pipeline (3 terminals)

```powershell
# Terminal 1 — GitHub Events producer
make producer

# Terminal 2 — Spark Streaming (Kafka → local GCS)
make streaming

# Terminal 3 — Export mart data for Power BI
make powerbi-demo     # no Snowflake needed, generates demo CSVs
```

Open **Power BI Desktop** → Get Data → Text/CSV → select files from `dashboard/exports/`.
For live data, connect directly to Snowflake (see Dashboard section below).

---

## 🔄 Daily Batch Pipeline

Once enough raw data has accumulated, run the full batch:

```powershell
# 1. Batch process yesterday's raw Parquet
make batch

# 2. Load to BigQuery (requires GCP project)
make bq-setup   # first time only
make bq-load

# 3. Run dbt transformations (local DuckDB, no GCP needed)
make dbt-all

# 4. For production BigQuery target:
make dbt-prod
```

---

## 🔧 dbt

The dbt project transforms raw events into clean analytics tables:

| Model | Type | Description |
|-------|------|-------------|
| `stg_github_events` | View | All events, typed & cleaned |
| `stg_push_events` | View | PushEvents with commit metadata |
| `stg_pr_events` | View | PR lifecycle events |
| `stg_watch_events` | View | Star events |
| `fact_commits` | Table | One row per push event |
| `fact_prs` | Table | One row per PR event (open/closed/merged) |
| `dim_repos` | Table | One row per repo (latest snapshot + activity rollup) |
| `dim_contributors` | Table | One row per GitHub actor |

**Dev (local DuckDB):**
```powershell
make dbt-all
```

**Prod (BigQuery):**
```powershell
make dbt-prod
```

---

## 📊 Dashboard (Power BI)

Power BI Desktop is the dashboard layer. Two connection options:

**Option A — Direct Snowflake connector (recommended for production)**
1. Open Power BI Desktop → Home → Get Data → Snowflake
2. Server: `<account>.snowflakecomputing.com`
3. Database: `GITHUB_ANALYTICS` / Schema: `GITHUB_MART`
4. Load tables: `dim_repos`, `dim_contributors`, `fact_commits`, `fact_prs`

**Option B — CSV export (local dev / demo)**
```powershell
make powerbi-demo     # generates demo CSVs in dashboard/exports/
make powerbi-export   # exports live Snowflake data to CSVs
```
Then in Power BI: Home → Get Data → Text/CSV → select files from `dashboard/exports/`.

Suggested visuals to build:
- **KPI cards** — total events, commits, PRs merged, contributors
- **Bar chart** — top repos by event volume
- **Scatter/bubble** — stars vs events (bubble size = contributors)
- **Line chart** — daily commits by language (14-day window)
- **Donut chart** — events by language
- **Bar chart** — PR funnel (open / closed / merged)
- **Table** — contributor leaderboard (top 20)

---

## 🎛️ Airflow DAG: `github_analytics_pipeline`

Schedule: `30 0 * * *` (00:30 UTC daily)

```
check_kafka_health
       │
       ▼
spark_batch_process    ← processes yesterday's raw GCS Parquet
       │
       ▼
bq_load_raw            ← loads to BigQuery raw table
       │
       ▼
dbt_run                ← dbt run --target prod
       │
       ▼
dbt_test               ← dbt test --target prod
       │
       ▼
pipeline_report        ← always runs, logs summary
```

---

## 🌍 Production (Real GCP)

1. Create a GCP project, enable BigQuery + GCS APIs
2. Create a Service Account with `BigQuery Admin` + `Storage Admin`
3. Download JSON key → `bigquery/service_account.json`
4. Update `.env`:
   ```env
   GCS_EMULATOR_HOST=       # leave empty for real GCS
   GCS_BUCKET=your-bucket-name
   GCP_PROJECT_ID=your-project-id
   GOOGLE_APPLICATION_CREDENTIALS=./bigquery/service_account.json
   DBT_TARGET=prod
   ```
5. `make bq-setup` — creates dataset + table
6. `make dbt-prod` — runs against BigQuery mart

---

## 📦 Tech Stack

| Layer | Technology |
|-------|------------|
| **Data Source** | GitHub Public Events API |
| **Streaming** | Apache Kafka (Confluent Platform 7.6) |
| **Stream Processing** | Apache Spark Structured Streaming 3.5 |
| **Object Storage** | Google Cloud Storage (fake-gcs-server for dev) |
| **Batch Processing** | Apache Spark Batch (PySpark 3.5 / pandas fallback) |
| **Data Warehouse** | Google BigQuery (partitioned + clustered) |
| **Transformations** | dbt (BigQuery adapter + DuckDB dev adapter) |
| **Orchestration** | Apache Airflow 2.9 |
| **Dashboard** | Power BI Desktop (Snowflake connector) |
| **Containerisation** | Docker + Docker Compose |

---

*Built as a Data Engineering portfolio project demonstrating Kafka streaming, Spark structured streaming + batch, GCS object storage, BigQuery warehousing, dbt transformations, and Airflow orchestration.*
