"""
GitHub Daily Event Fetcher
==========================
Fetches GitHub public events directly from the API and writes them as
processed Parquet files ready for Snowflake loading.

This replaces the Kafka + Spark Streaming components for Railway deployment,
where the full streaming stack cannot run 24/7.

Usage:
    python producer/github_daily_fetcher.py
    python producer/github_daily_fetcher.py --date 2026-06-08
    python producer/github_daily_fetcher.py --date 2026-06-08 --max-pages 10
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

GITHUB_TOKEN   = os.getenv("GITHUB_TOKEN", "")
LOCAL_GCS_ROOT = os.getenv("LOCAL_GCS_ROOT", "./data/gcs-emulator")
GCS_BUCKET     = os.getenv("GCS_BUCKET", "github-analytics-raw")

# GitHub API returns max 30 events per page, 10 pages max = 300 events
DEFAULT_MAX_PAGES = 10

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>fetcher</cyan> | {message}",
    colorize=True,
)
logger.add("logs/fetcher.log", rotation="50 MB", retention="14 days")
os.makedirs("logs", exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────

def _headers() -> dict:
    h = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def fetch_public_events(max_pages: int = DEFAULT_MAX_PAGES) -> list[dict]:
    """Fetch recent public GitHub events (up to max_pages * 30 events)."""
    all_events: list[dict] = []

    for page in range(1, max_pages + 1):
        url = f"https://api.github.com/events?per_page=30&page={page}"
        try:
            resp = requests.get(url, headers=_headers(), timeout=30)
        except requests.RequestException as exc:
            logger.warning(f"Request failed on page {page}: {exc} — stopping")
            break

        if resp.status_code == 422:
            # GitHub returns 422 when page is beyond available data
            break
        if resp.status_code == 403:
            logger.error("GitHub API rate limit hit — use a GITHUB_TOKEN in .env")
            break
        resp.raise_for_status()

        page_data = resp.json()
        if not page_data:
            break

        all_events.extend(page_data)
        logger.info(f"  Page {page}: +{len(page_data)} events  (total so far: {len(all_events)})")

        remaining = int(resp.headers.get("X-RateLimit-Remaining", 999))
        if remaining < 5:
            logger.warning(f"Rate limit nearly exhausted ({remaining} left) — stopping early")
            break

        time.sleep(0.3)  # be polite to the API

    return all_events


# ─────────────────────────────────────────────────────────────────────────────
# Parsing
# ─────────────────────────────────────────────────────────────────────────────

def _parse_event(raw: dict) -> dict:
    """Flatten a raw GitHub API event into the pipeline schema."""
    payload    = raw.get("payload", {}) or {}
    actor      = raw.get("actor",   {}) or {}
    repo       = raw.get("repo",    {}) or {}
    org        = raw.get("org",     {}) or {}
    event_type = raw.get("type", "")
    now_iso    = datetime.now(timezone.utc).isoformat()

    repo_name  = repo.get("name", "")
    repo_owner = repo_name.split("/")[0] if "/" in repo_name else ""

    # ── Text content ──────────────────────────────────────────────────────────
    if event_type == "PushEvent":
        commits      = payload.get("commits", []) or []
        commit_count = len(commits)
        text_content = " | ".join(
            c.get("message", "")[:100] for c in commits[:5]
        )[:500]
    elif event_type == "PullRequestEvent":
        pr           = payload.get("pull_request", {}) or {}
        commit_count = None
        text_content = f"{pr.get('title', '')} {pr.get('body', '') or ''}"[:500]
    elif event_type == "IssuesEvent":
        issue        = payload.get("issue", {}) or {}
        commit_count = None
        text_content = f"{issue.get('title', '')} {issue.get('body', '') or ''}"[:500]
    else:
        commit_count = None
        text_content = ""

    # ── PR fields ─────────────────────────────────────────────────────────────
    pr_merged = False
    pr_number = None
    pr_state  = None
    if event_type == "PullRequestEvent":
        pr        = payload.get("pull_request", {}) or {}
        pr_merged = bool(pr.get("merged", False))
        pr_number = pr.get("number")
        if pr_merged:
            pr_state = "merged"
        else:
            pr_state = pr.get("state", "open")

    # ── Issue fields ──────────────────────────────────────────────────────────
    issue_number = None
    if event_type == "IssuesEvent":
        issue        = payload.get("issue", {}) or {}
        issue_number = issue.get("number")

    return {
        "event_id":     raw.get("id", ""),
        "event_type":   event_type,
        "action":       payload.get("action", ""),
        "actor":        actor.get("login", ""),
        "org":          org.get("login", "") if org else "",
        "repo_name":    repo_name,
        "repo_owner":   repo_owner,
        # language/stars/forks not available in public events feed without extra API calls
        "language":     None,
        "stars":        None,
        "forks":        None,
        "open_issues":  None,
        "description":  None,
        "is_tracked":   False,
        "topics":       "[]",
        "text_content": text_content.strip(),
        "commit_count": commit_count,
        "pr_merged":    pr_merged,
        "pr_number":    pr_number,
        "issue_number": issue_number,
        "ref_type":     payload.get("ref_type"),
        "ref_name":     payload.get("ref"),
        "is_merge":     pr_merged and event_type == "PullRequestEvent",
        "has_text":     bool(text_content.strip()),
        "pr_state":     pr_state,
        "created_at":   raw.get("created_at"),
        "ingested_at":  now_iso,
        "processed_at": now_iso,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Write
# ─────────────────────────────────────────────────────────────────────────────

def write_parquet(records: list[dict], date: str) -> int:
    """Write records to processed Parquet partition for the given date."""
    output_dir = (
        Path(LOCAL_GCS_ROOT) / GCS_BUCKET / f"processed/date={date}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(records)

    # Timestamps → microsecond precision (Snowflake requirement)
    for col in ["created_at", "ingested_at", "processed_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
            df[col] = df[col].astype("datetime64[us, UTC]")

    # Nullable integers
    for col in ["stars", "forks", "open_issues", "commit_count", "pr_number", "issue_number"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int64")

    # Booleans
    for col in ["is_tracked", "pr_merged", "is_merge", "has_text"]:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)

    df["date_partition"] = pd.to_datetime(date).date()
    df = df.drop_duplicates(subset=["event_id"])

    out_path = output_dir / "events.parquet"
    df.to_parquet(out_path, index=False, compression="snappy")
    logger.success(f"Written {len(df):,} rows → {out_path}")
    return len(df)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch GitHub public events and write processed Parquet"
    )
    parser.add_argument(
        "--date",
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Date label for the output partition (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Pages of GitHub events to fetch (default: {DEFAULT_MAX_PAGES})",
    )
    args = parser.parse_args()

    logger.info(f"GitHub Daily Fetcher — date={args.date}  max_pages={args.max_pages}")

    if not GITHUB_TOKEN:
        logger.warning("GITHUB_TOKEN not set — unauthenticated requests are rate-limited to 60/hour")

    raw_events = fetch_public_events(max_pages=args.max_pages)
    if not raw_events:
        logger.error("No events fetched — aborting")
        sys.exit(1)

    records = [_parse_event(e) for e in raw_events]
    count   = write_parquet(records, args.date)

    logger.success(f"Done — {count} events saved for partition date={args.date}")


if __name__ == "__main__":
    main()
