"""
GitHub Analytics Pipeline — Power BI Data Export
=================================================
Exports Snowflake mart tables to CSV files for consumption by Power BI Desktop.

Power BI connects to these CSVs via:
  Get Data → Text/CSV → point to dashboard/exports/*.csv

For live production data, connect Power BI directly to Snowflake:
  Get Data → Snowflake → enter account/warehouse/database credentials
  Tables: MART.fact_commits, fact_prs, dim_repos, dim_contributors

Usage:
    python dashboard/export_for_powerbi.py              # exports to dashboard/exports/
    python dashboard/export_for_powerbi.py --demo       # generates demo data (no Snowflake needed)
    python dashboard/export_for_powerbi.py --days 30    # last N days of commit/PR data (default: 14)

Environment (for live Snowflake export):
    SNOWFLAKE_ACCOUNT       Your Snowflake account identifier
    SNOWFLAKE_USER          Snowflake username
    SNOWFLAKE_PASSWORD      Snowflake password
    SNOWFLAKE_WAREHOUSE     Warehouse name (default: COMPUTE_WH)
    SNOWFLAKE_DATABASE      Database name (default: GITHUB_PIPELINE)
    SNOWFLAKE_SCHEMA_MART   Mart schema (default: MART)
"""

import argparse
import os
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

SF_ACCOUNT     = os.getenv("SNOWFLAKE_ACCOUNT", "")
SF_USER        = os.getenv("SNOWFLAKE_USER", "")
SF_PASSWORD    = os.getenv("SNOWFLAKE_PASSWORD", "")
SF_WAREHOUSE   = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE    = os.getenv("SNOWFLAKE_DATABASE", "GITHUB_PIPELINE")
SF_SCHEMA_MART = os.getenv("SNOWFLAKE_SCHEMA_MART", "MART")

EXPORT_DIR = Path(__file__).parent / "exports"


# ── Snowflake helpers ─────────────────────────────────────────────────────────

def _sf_query(sql: str) -> pd.DataFrame:
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA_MART,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


def fetch_dim_repos() -> pd.DataFrame:
    return _sf_query(f"""
        SELECT repo_name, repo_owner, language, stars, forks, open_issues,
               total_events, total_pushes, total_pr_events, total_stars,
               unique_contributors, is_tracked, last_seen_at
        FROM {SF_DATABASE}.{SF_SCHEMA_MART}.dim_repos
        ORDER BY total_events DESC
        LIMIT 200
    """)


def fetch_dim_contributors() -> pd.DataFrame:
    return _sf_query(f"""
        SELECT actor, total_events, repos_contributed_to,
               push_events, pr_events, prs_merged, total_commits,
               primary_language, most_recent_repo, first_seen_at, last_seen_at
        FROM {SF_DATABASE}.{SF_SCHEMA_MART}.dim_contributors
        ORDER BY total_events DESC
        LIMIT 100
    """)


def fetch_fact_commits(days: int) -> pd.DataFrame:
    return _sf_query(f"""
        SELECT event_date, language,
               COUNT(*)          AS push_events,
               SUM(commit_count) AS total_commits
        FROM {SF_DATABASE}.{SF_SCHEMA_MART}.fact_commits
        WHERE event_date >= DATEADD(day, -{days}, CURRENT_DATE())
        GROUP BY event_date, language
        ORDER BY event_date
    """)


def fetch_fact_prs(days: int) -> pd.DataFrame:
    return _sf_query(f"""
        SELECT pr_status,
               COUNT(DISTINCT event_id)  AS events,
               COUNT(DISTINCT pr_number) AS unique_prs
        FROM {SF_DATABASE}.{SF_SCHEMA_MART}.fact_prs
        WHERE event_date >= DATEADD(day, -{days}, CURRENT_DATE())
        GROUP BY pr_status
    """)


# ── Demo data ─────────────────────────────────────────────────────────────────

def demo_dim_repos() -> pd.DataFrame:
    cfg = [
        ("microsoft/vscode",        "TypeScript", 158000, 27000, 4200, 180, 35),
        ("torvalds/linux",           "C",          170000, 51000, 2800,  90, 20),
        ("huggingface/transformers", "Python",     128000, 25000, 5100, 220, 60),
        ("openai/openai-python",     "Python",      22000,  2100, 3300, 150, 45),
        ("rust-lang/rust",           "Rust",        95000, 12000, 1900,  70, 30),
        ("golang/go",                "Go",         121000, 17000, 2100,  85, 25),
        ("tensorflow/tensorflow",    "Python",     182000, 74000, 3800, 140, 40),
        ("facebook/react",           "JavaScript", 225000, 45000, 4500, 200, 55),
        ("vuejs/vue",                "JavaScript", 207000, 33000, 3100, 110, 35),
        ("django/django",            "Python",      79000, 31000, 2400,  95, 28),
    ]
    rows = []
    for repo, lang, stars, forks, evts, pushes, contribs in cfg:
        rows.append({
            "repo_name": repo, "repo_owner": repo.split("/")[0], "language": lang,
            "stars": stars, "forks": forks, "open_issues": random.randint(100, 2000),
            "total_events": evts, "total_pushes": pushes,
            "total_pr_events": int(evts * 0.15), "total_stars": int(evts * 0.3),
            "unique_contributors": contribs, "is_tracked": True,
            "last_seen_at": datetime.now(timezone.utc).isoformat(),
        })
    return pd.DataFrame(rows)


def demo_dim_contributors() -> pd.DataFrame:
    langs = ["Python", "TypeScript", "Go", "Rust", "JavaScript", "C", "C++"]
    repos = ["microsoft/vscode", "torvalds/linux", "huggingface/transformers",
             "openai/openai-python", "rust-lang/rust"]
    rows = []
    for i in range(1, 51):
        total = random.randint(10, 500)
        rows.append({
            "actor": f"dev_{i:04d}",
            "total_events": total,
            "repos_contributed_to": random.randint(1, 8),
            "push_events": int(total * 0.5),
            "pr_events": int(total * 0.2),
            "prs_merged": int(total * 0.1),
            "total_commits": int(total * 2.5),
            "primary_language": random.choice(langs),
            "most_recent_repo": random.choice(repos),
            "first_seen_at": (datetime.now(timezone.utc) - timedelta(days=random.randint(1, 30))).isoformat(),
            "last_seen_at": datetime.now(timezone.utc).isoformat(),
        })
    return pd.DataFrame(rows).sort_values("total_events", ascending=False)


def demo_fact_commits(days: int) -> pd.DataFrame:
    langs = ["Python", "TypeScript", "Go", "Rust", "JavaScript"]
    rows = []
    for i in range(days):
        d = (datetime.now(timezone.utc) - timedelta(days=i)).date().isoformat()
        for lang in langs:
            rows.append({
                "event_date": d, "language": lang,
                "push_events": random.randint(20, 200),
                "total_commits": random.randint(50, 600),
            })
    return pd.DataFrame(rows)


def demo_fact_prs() -> pd.DataFrame:
    return pd.DataFrame([
        {"pr_status": "open",   "events": 142, "unique_prs": 98},
        {"pr_status": "closed", "events":  87, "unique_prs": 61},
        {"pr_status": "merged", "events": 203, "unique_prs": 175},
    ])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fix_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert float columns that contain only whole numbers to Int64.
    This prevents Power BI from displaying integer values as decimals."""
    for col in df.select_dtypes(include="float").columns:
        non_null = df[col].dropna()
        if (non_null == non_null.round()).all():
            df[col] = df[col].astype("Int64")
    return df


# ── Export ────────────────────────────────────────────────────────────────────

def export(df: pd.DataFrame, name: str) -> Path:
    path = EXPORT_DIR / f"{name}.csv"
    df = _fix_numeric_types(df)
    df.to_csv(path, index=False)
    print(f"  ✓ {name}.csv  ({len(df):,} rows)")
    return path


def main():
    parser = argparse.ArgumentParser(description="Export Snowflake mart tables to CSV for Power BI")
    parser.add_argument("--demo", action="store_true", help="Generate demo data (no Snowflake required)")
    parser.add_argument("--days", type=int, default=14, help="Days of commit/PR history to export (default: 14)")
    args = parser.parse_args()

    use_demo = args.demo or not (SF_ACCOUNT and SF_USER and SF_PASSWORD)
    if use_demo and not args.demo:
        print("⚠  Snowflake credentials not set — falling back to demo data.")
        print("   Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD in .env for live export.\n")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Exporting {'demo' if use_demo else 'Snowflake'} data to {EXPORT_DIR}/\n")

    if use_demo:
        export(demo_dim_repos(),            "dim_repos")
        export(demo_dim_contributors(),     "dim_contributors")
        export(demo_fact_commits(args.days),"fact_commits")
        export(demo_fact_prs(),             "fact_prs")
    else:
        export(fetch_dim_repos(),            "dim_repos")
        export(fetch_dim_contributors(),     "dim_contributors")
        export(fetch_fact_commits(args.days),"fact_commits")
        export(fetch_fact_prs(args.days),    "fact_prs")

    print(f"\nDone. Open Power BI Desktop and load files from:\n  {EXPORT_DIR.resolve()}")
    print("\nOr connect Power BI directly to Snowflake:")
    print("  Home → Get Data → Snowflake")
    print(f"  Server:   {SF_ACCOUNT or '<your-account>.snowflakecomputing.com'}")
    print(f"  Database: {SF_DATABASE}")
    print(f"  Schema:   {SF_SCHEMA_MART}")
    print("  Tables:   dim_repos, dim_contributors, fact_commits, fact_prs")


if __name__ == "__main__":
    main()
