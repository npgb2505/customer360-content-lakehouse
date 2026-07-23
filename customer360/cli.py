"""Command line entry point for generation, incremental loads, and backfills."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import date, timedelta
from pathlib import Path

from customer360.generator import generate_daily_partition, generate_master_data
from customer360.pipeline import create_spark, run_partition


def _dates(start: date, days: int) -> list[date]:
    return [start + timedelta(days=offset) for offset in range(days)]


def _clear_directory(path: Path) -> None:
    """Remove generated children while preserving a bind-mounted root directory."""
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--root", type=Path, default=Path("data"))
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2025, 1, 30))
    parser.add_argument("--days", type=int, default=4)
    parser.add_argument("--watch-events", type=int, default=1_000)
    parser.add_argument("--search-events", type=int, default=120)
    parser.add_argument("--invalid-events", type=int, default=5)
    parser.add_argument("--duplicate-events", type=int, default=2)
    parser.add_argument("--users", type=int, default=500)
    parser.add_argument("--contents", type=int, default=120)
    parser.add_argument("--seed", type=int, default=2026)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    demo = subparsers.add_parser("demo", help="Reset and execute a deterministic demo")
    _add_common(demo)
    generate = subparsers.add_parser("generate", help="Generate raw input partitions")
    _add_common(generate)
    run = subparsers.add_parser("run", help="Process existing raw partitions")
    run.add_argument("--root", type=Path, default=Path("data"))
    run.add_argument("--start-date", type=date.fromisoformat, required=True)
    run.add_argument("--days", type=int, default=1)
    run.add_argument(
        "--mode",
        choices=("incremental", "backfill", "full-reload"),
        default="incremental",
    )
    return parser


def _generate(args: argparse.Namespace) -> None:
    generate_master_data(args.root, users=args.users, contents=args.contents, seed=args.seed)
    for current_date in _dates(args.start_date, args.days):
        generate_daily_partition(
            args.root,
            current_date,
            watch_events=args.watch_events,
            search_events=args.search_events,
            users=args.users,
            contents=args.contents,
            invalid_events=args.invalid_events,
            duplicate_events=args.duplicate_events,
            seed=args.seed,
        )


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "demo":
        _clear_directory(args.root)
        for generated in (Path("artifacts"), Path("powerbi") / "exports"):
            _clear_directory(generated)
        _generate(args)
    elif args.command == "generate":
        _generate(args)

    if args.command in {"demo", "run"}:
        if args.command == "run" and args.mode == "full-reload":
            lakehouse = args.root / "lakehouse"
            if lakehouse.exists():
                shutil.rmtree(lakehouse)
        spark = create_spark()
        try:
            result = None
            for current_date in _dates(args.start_date, args.days):
                result = run_partition(spark, args.root, current_date)
            print(json.dumps(result, indent=2))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
