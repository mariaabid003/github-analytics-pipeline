"""
GitHub Events Kafka Producer
=============================
Polls the GitHub Public Events API and publishes enriched event records
to Kafka topic `github-events`.

GitHub API polling strategy:
  - GET /events?per_page=100  (public timeline, up to 300 events across 3 pages)
  - Polls every POLL_INTERVAL_SECONDS (default 5s)
  - Deduplicates by event_id to avoid re-publishing
  - Rate limit with PAT: 5000 req/hr → safe at 1 req/5s

Event types tracked:
  WatchEvent    → repo starred
  ForkEvent     → repo forked
  PushEvent     → commits pushed (VADER analyses commit messages)
  PullRequestEvent → PR opened/closed/merged
  IssuesEvent   → issue opened/closed/labelled
  ReleaseEvent  → new release published
  CreateEvent   → branch/tag created

Usage:
    python producer/github_producer.py

Environment Variables (.env):
    GITHUB_TOKEN                   — Personal Access Token (increases rate limit 5000×)
    KAFKA_BOOTSTRAP_SERVERS
    KAFKA_TOPIC_GITHUB             — default: github-events
    GITHUB_POLL_INTERVAL_SECONDS   — default: 5
    GITHUB_PAGES                   — number of pages to fetch per poll (1–3)
    GITHUB_TRACK_REPOS             — comma-separated "owner/repo" for targeted tracking
"""

import json
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

# ── Load env ──────────────────────────────────────────────────────────────────
load_dotenv()

GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")
KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC_GITHUB", "github-events")
POLL_INTERVAL   = int(os.getenv("GITHUB_POLL_INTERVAL_SECONDS", "5"))
PAGES_PER_POLL  = int(os.getenv("GITHUB_PAGES", "3"))

TRACK_REPOS_RAW = os.getenv(
    "GITHUB_TRACK_REPOS",
    "torvalds/linux,microsoft/vscode,openai/openai-python,"
    "huggingface/transformers,golang/go,rust-lang/rust,"
    "tensorflow/tensorflow,facebook/react,vuejs/vue,django/django",
)
TRACKED_REPOS = set(r.strip() for r in TRACK_REPOS_RAW.split(",") if r.strip())

# ── GitHub API ────────────────────────────────────────────────────────────────
GITHUB_API   = "https://api.github.com"
HEADERS      = {
    "Accept":               "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

# ── Event types of interest ───────────────────────────────────────────────────
WATCHED_TYPES = {
    "WatchEvent", "ForkEvent", "PushEvent",
    "PullRequestEvent", "IssuesEvent", "ReleaseEvent",
    "CreateEvent", "IssueCommentEvent",
}

# ── Logging ───────────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>github_producer</cyan> | {message}",
    colorize=True,
)
os.makedirs("logs", exist_ok=True)
logger.add("logs/github_producer.log", rotation="100 MB", retention="7 days", level="DEBUG")


# ─────────────────────────────────────────────────────────────────────────────
# GitHub API helpers
# ─────────────────────────────────────────────────────────────────────────────

def gh_get(path: str, params: dict = None) -> Optional[list | dict]:
    """Make a GitHub API request with rate-limit handling."""
    url = f"{GITHUB_API}{path}"
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)

        remaining = int(resp.headers.get("X-RateLimit-Remaining", 999))
        if remaining < 10:
            reset_ts = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = max(0, reset_ts - time.time()) + 2
            logger.warning(f"Rate limit low ({remaining} left) — sleeping {wait:.0f}s")
            time.sleep(wait)

        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 304:   # Not Modified (ETag match)
            return []
        elif resp.status_code == 403:
            logger.warning(f"403 Forbidden — rate limited: {resp.text[:200]}")
            time.sleep(60)
            return []
        else:
            logger.warning(f"GitHub API {resp.status_code}: {url}")
            return []

    except requests.RequestException as exc:
        logger.error(f"Request failed: {exc}")
        return []


def fetch_repo_meta(repo_name: str) -> dict:
    """Fetch repo metadata (language, stars, forks). Cached per-run."""
    data = gh_get(f"/repos/{repo_name}") or {}
    return {
        "language":    data.get("language", "Unknown"),
        "stars":       data.get("stargazers_count", 0),
        "forks":       data.get("forks_count", 0),
        "open_issues": data.get("open_issues_count", 0),
        "topics":      data.get("topics", []),
        "description": (data.get("description") or "")[:200],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Event payload normaliser
# ─────────────────────────────────────────────────────────────────────────────

def normalise_event(raw: dict, repo_meta_cache: dict) -> Optional[dict]:
    """
    Convert a raw GitHub API event into a standardised, Kafka-ready payload.
    Returns None for event types we don't care about.
    """
    event_type = raw.get("type", "")
    if event_type not in WATCHED_TYPES:
        return None

    repo_name  = raw.get("repo", {}).get("name", "")
    actor      = raw.get("actor", {}).get("login", "")
    payload    = raw.get("payload", {})
    created_at = raw.get("created_at", datetime.now(timezone.utc).isoformat())
    org        = (raw.get("org") or {}).get("login", "")

    # Fetch or reuse repo metadata
    if repo_name not in repo_meta_cache:
        repo_meta_cache[repo_name] = fetch_repo_meta(repo_name)
    meta = repo_meta_cache[repo_name]

    # ── Extract text for NLP (commit messages / PR / issue titles) ────────────
    text_content = ""
    action       = payload.get("action", "")
    pr_merged    = False
    pr_number    = None
    issue_number = None
    commit_count = 0
    ref_type     = ""
    ref_name     = ""

    if event_type == "PushEvent":
        commits      = payload.get("commits", [])
        # 'size' is the total number of commits in the push (authoritative).
        # GitHub only returns up to 20 commits in the array, so len(commits)
        # undercounts for large pushes. Fall back to len(commits) if size missing.
        commit_count = payload.get("size", 0) or len(commits)
        messages     = [c.get("message", "") for c in commits[:5]]
        text_content = " | ".join(messages)

    elif event_type == "PullRequestEvent":
        pr = payload.get("pull_request", {})
        text_content = f"{pr.get('title', '')} {pr.get('body', '') or ''}"[:500]
        pr_merged    = pr.get("merged", False)
        pr_number    = pr.get("number")

    elif event_type == "IssuesEvent":
        issue        = payload.get("issue", {})
        text_content = f"{issue.get('title', '')} {issue.get('body', '') or ''}"[:500]
        issue_number = issue.get("number")

    elif event_type == "ReleaseEvent":
        rel          = payload.get("release", {})
        text_content = f"{rel.get('name', '')} {rel.get('body', '') or ''}"[:500]
        action       = rel.get("action", action)

    elif event_type == "IssueCommentEvent":
        comment      = payload.get("comment", {})
        text_content = (comment.get("body", "") or "")[:300]

    elif event_type == "CreateEvent":
        ref_type = payload.get("ref_type", "")
        ref_name = payload.get("ref", "")

    owner = repo_name.split("/")[0] if "/" in repo_name else ""

    return {
        # Identifiers
        "event_id":      raw.get("id", ""),
        "event_type":    event_type,
        "action":        action,

        # Actor
        "actor":         actor,
        "org":           org,

        # Repo
        "repo_name":     repo_name,
        "repo_owner":    owner,
        "language":      meta.get("language", "Unknown"),
        "stars":         meta.get("stars", 0),
        "forks":         meta.get("forks", 0),
        "open_issues":   meta.get("open_issues", 0),
        "description":   meta.get("description", ""),
        "topics":        meta.get("topics", []),

        # Event specifics
        "text_content":  text_content.strip(),
        "commit_count":  commit_count,
        "pr_merged":     pr_merged,
        "pr_number":     pr_number,
        "issue_number":  issue_number,
        "ref_type":      ref_type,
        "ref_name":      ref_name,

        # Is this a specifically tracked repo?
        "is_tracked":    repo_name in TRACKED_REPOS,

        # Timestamps
        "created_at":    created_at,
        "ingested_at":   datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Producer
# ─────────────────────────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=2, min=2, max=30), reraise=True)
def create_kafka_producer() -> KafkaProducer:
    logger.info(f"Connecting to Kafka at {KAFKA_SERVERS} ...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        max_block_ms=10_000,
        compression_type="gzip",
    )
    logger.success("Kafka producer connected ✓")
    return producer


def on_send_success(meta):
    logger.debug(f"✓ topic={meta.topic} partition={meta.partition} offset={meta.offset}")

def on_send_error(exc):
    logger.error(f"Kafka send error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Main producer class
# ─────────────────────────────────────────────────────────────────────────────

class GitHubProducer:
    def __init__(self):
        self.producer        = create_kafka_producer()
        self.seen_ids        = deque(maxlen=50_000)   # dedup ring-buffer
        self.seen_set        = set()
        self.repo_meta_cache = {}
        self.stats           = {"sent": 0, "skipped_dup": 0, "skipped_type": 0, "errors": 0}
        self.running         = True

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        logger.info(f"Tracking {len(TRACKED_REPOS)} specific repos + public timeline")
        logger.info(f"Watched event types: {sorted(WATCHED_TYPES)}")

    def _is_duplicate(self, event_id: str) -> bool:
        if event_id in self.seen_set:
            return True
        # Evict oldest from set when ring-buffer wraps
        if len(self.seen_ids) == self.seen_ids.maxlen:
            oldest = self.seen_ids[0]
            self.seen_set.discard(oldest)
        self.seen_ids.append(event_id)
        self.seen_set.add(event_id)
        return False

    def _publish(self, payload: dict):
        key = payload["repo_name"]
        (
            self.producer
            .send(KAFKA_TOPIC, key=key, value=payload)
            .add_callback(on_send_success)
            .add_errback(on_send_error)
        )
        self.stats["sent"] += 1

    def _fetch_public_events(self) -> list[dict]:
        """Fetch public GitHub event pages."""
        events = []
        for page in range(1, PAGES_PER_POLL + 1):
            page_events = gh_get("/events", params={"per_page": 100, "page": page}) or []
            events.extend(page_events)
            if len(page_events) < 100:
                break   # no more pages
        return events

    def _fetch_tracked_repo_events(self) -> list[dict]:
        """Fetch events for specifically tracked repos."""
        events = []
        for repo in TRACKED_REPOS:
            repo_events = gh_get(f"/repos/{repo}/events", params={"per_page": 30}) or []
            events.extend(repo_events)
        return events

    def _process_events(self, events: list[dict]):
        """Normalise and publish a batch of raw GitHub events."""
        for raw in events:
            event_id = raw.get("id", "")
            if self._is_duplicate(event_id):
                self.stats["skipped_dup"] += 1
                continue

            payload = normalise_event(raw, self.repo_meta_cache)
            if payload is None:
                self.stats["skipped_type"] += 1
                continue

            self._publish(payload)

            icon = {
                "WatchEvent":       "⭐",
                "ForkEvent":        "🍴",
                "PushEvent":        "📦",
                "PullRequestEvent": "🔀",
                "IssuesEvent":      "🐛",
                "ReleaseEvent":     "🚀",
                "CreateEvent":      "🌿",
                "IssueCommentEvent": "💬",
            }.get(payload["event_type"], "📡")

            logger.info(
                f"{icon} [{payload['event_type']:<20}] "
                f"{payload['repo_name']:<45} | "
                f"⭐{payload['stars']:>6} | "
                f"lang={payload['language']}"
            )

    def _shutdown(self, *_):
        logger.info("Shutting down GitHub producer ...")
        self.running = False
        self.producer.flush()
        self.producer.close()
        logger.info(f"Final stats: {self.stats}")
        sys.exit(0)

    def run(self):
        """Main polling loop."""
        if not GITHUB_TOKEN:
            logger.warning("No GITHUB_TOKEN set — using unauthenticated API (60 req/hr limit!)")
            logger.warning("Get a free token at: https://github.com/settings/tokens")
        else:
            logger.success("GitHub PAT configured — 5000 req/hr rate limit ✓")

        logger.info(f"Starting poll loop (every {POLL_INTERVAL}s) ...")

        poll_count = 0
        while self.running:
            try:
                # Alternate between public timeline and tracked repos
                if poll_count % 2 == 0:
                    events = self._fetch_public_events()
                    source = "public timeline"
                else:
                    events = self._fetch_tracked_repo_events()
                    source = "tracked repos"

                logger.info(f"Poll #{poll_count} ({source}): {len(events)} raw events")
                self._process_events(events)

                if self.stats["sent"] % 100 == 0 and self.stats["sent"] > 0:
                    self.producer.flush()
                    logger.info(f"📊 Stats → {self.stats} | Repo cache: {len(self.repo_meta_cache)} repos")

                poll_count += 1
                time.sleep(POLL_INTERVAL)

            except Exception as exc:
                self.stats["errors"] += 1
                logger.error(f"Poll error: {exc}")
                time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    producer = GitHubProducer()
    producer.run()
