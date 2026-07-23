"""Deterministic synthetic data generator for the Customer 360 demo."""

from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PLANS = (
    ("Mobile", 1, 5.99),
    ("Standard", 6, 12.99),
    ("Premium", 12, 19.99),
)
REGIONS = ("North", "Central", "South")
DEVICES = ("Smart TV", "Mobile", "Desktop", "Tablet")
CATEGORIES = ("Drama", "Comedy", "Action", "Documentary", "Kids", "Sports")
QUERY_TERMS = (
    "action movies",
    "family comedy",
    "nature documentary",
    "football highlights",
    "new drama",
    "kids animation",
    "award winners",
    "weekend movies",
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_master_data(
    root: Path,
    *,
    users: int = 500,
    contents: int = 120,
    seed: int = 2026,
) -> tuple[Path, Path]:
    """Create stable customer and content dimensions."""
    rng = random.Random(seed)
    master = root / "raw" / "master"
    customer_rows: list[dict[str, object]] = []
    for index in range(1, users + 1):
        plan_name, months, fee = PLANS[(index - 1) % len(PLANS)]
        customer_rows.append(
            {
                "customer_id": f"C{index:06d}",
                "full_name": f"Customer {index:06d}",
                "plan_type": plan_name,
                "contract_months": months,
                "monthly_fee_usd": fee,
                "region": REGIONS[rng.randrange(len(REGIONS))],
                "signup_date": (date(2022, 1, 1) + timedelta(days=rng.randrange(1000))).isoformat(),
                "status": "active" if rng.random() > 0.04 else "paused",
            }
        )

    content_rows: list[dict[str, object]] = []
    for index in range(1, contents + 1):
        category = CATEGORIES[(index - 1) % len(CATEGORIES)]
        content_rows.append(
            {
                "content_id": f"M{index:05d}",
                "title": f"{category} Title {index:04d}",
                "category": category,
                "duration_minutes": rng.randint(25, 150),
                "release_year": rng.randint(2014, 2026),
            }
        )

    users_path = master / "users.csv"
    content_path = master / "content.csv"
    _write_csv(users_path, list(customer_rows[0]), customer_rows)
    _write_csv(content_path, list(content_rows[0]), content_rows)
    return users_path, content_path


def generate_daily_partition(
    root: Path,
    event_date: date,
    *,
    watch_events: int = 1_000,
    search_events: int = 120,
    users: int = 500,
    contents: int = 120,
    invalid_events: int = 5,
    duplicate_events: int = 2,
    seed: int = 2026,
) -> tuple[Path, Path]:
    """Generate one replayable daily partition with declared bad and duplicate records."""
    if invalid_events >= min(watch_events, search_events):
        raise ValueError("invalid_events must be smaller than both event counts")

    day_seed = seed + int(event_date.strftime("%Y%m%d"))
    rng = random.Random(day_seed)
    base = datetime.combine(event_date, datetime.min.time(), tzinfo=timezone.utc)
    partition = root / "raw" / f"event_date={event_date.isoformat()}"
    content_durations: dict[str, int] = {}
    with (root / "raw" / "master" / "content.csv").open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            content_durations[row["content_id"]] = int(row["duration_minutes"])

    watch_rows: list[dict[str, object]] = []
    for index in range(watch_events):
        customer_number = rng.randint(1, users)
        content_number = rng.randint(1, contents)
        content_id = f"M{content_number:05d}"
        duration = content_durations[content_id]
        timestamp = base + timedelta(seconds=rng.randrange(86_400))
        row: dict[str, object] = {
            "event_id": f"W-{event_date:%Y%m%d}-{index:09d}",
            "customer_id": f"C{customer_number:06d}",
            "content_id": content_id,
            "session_id": f"S-{event_date:%Y%m%d}-{rng.randint(1, max(1, watch_events // 3)):08d}",
            "event_ts": timestamp.isoformat(),
            "device": DEVICES[rng.randrange(len(DEVICES))],
            "watched_minutes": round(rng.uniform(1, duration), 2),
            "ingest_ts": (timestamp + timedelta(seconds=rng.randint(1, 60))).isoformat(),
        }
        if index < invalid_events:
            if index % 2:
                row["watched_minutes"] = -5
            else:
                row["customer_id"] = "C_UNKNOWN"
        watch_rows.append(row)

    for index in range(min(duplicate_events, len(watch_rows))):
        watch_rows.append(dict(watch_rows[invalid_events + index]))

    search_rows: list[dict[str, object]] = []
    for index in range(search_events):
        timestamp = base + timedelta(seconds=rng.randrange(86_400))
        clicked = rng.random() < 0.42
        row = {
            "search_id": f"Q-{event_date:%Y%m%d}-{index:09d}",
            "customer_id": f"C{rng.randint(1, users):06d}",
            "query": QUERY_TERMS[rng.randrange(len(QUERY_TERMS))],
            "result_content_id": f"M{rng.randint(1, contents):05d}" if clicked else "",
            "clicked": str(clicked).lower(),
            "search_ts": timestamp.isoformat(),
            "device": DEVICES[rng.randrange(len(DEVICES))],
            "ingest_ts": (timestamp + timedelta(seconds=rng.randint(1, 30))).isoformat(),
        }
        if index < invalid_events:
            if index % 2:
                row["query"] = ""
            else:
                row["customer_id"] = "C_UNKNOWN"
        search_rows.append(row)

    for index in range(min(duplicate_events, len(search_rows))):
        search_rows.append(dict(search_rows[invalid_events + index]))

    watch_path = partition / "watch_events.csv"
    search_path = partition / "search_events.csv"
    _write_csv(watch_path, list(watch_rows[0]), watch_rows)
    _write_csv(search_path, list(search_rows[0]), search_rows)
    return watch_path, search_path
