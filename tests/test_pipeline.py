from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest

from customer360.generator import generate_daily_partition, generate_master_data
from customer360.pipeline import create_spark, run_partition


@pytest.fixture(scope="session")
def spark():
    session = create_spark("customer360-tests")
    yield session
    session.stop()


def test_generator_is_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    for target in (first, second):
        generate_master_data(target, users=20, contents=10)
        generate_daily_partition(
            target,
            date(2025, 2, 1),
            watch_events=30,
            search_events=10,
            users=20,
            contents=10,
            invalid_events=2,
            duplicate_events=1,
        )
    first_text = (first / "raw" / "event_date=2025-02-01" / "watch_events.csv").read_text()
    second_text = (second / "raw" / "event_date=2025-02-01" / "watch_events.csv").read_text()
    assert first_text == second_text


def test_pipeline_reconciles_and_is_idempotent(tmp_path: Path, spark) -> None:
    project = tmp_path / "project"
    root = project / "data"
    generate_master_data(root, users=30, contents=12)
    generate_daily_partition(
        root,
        date(2025, 2, 1),
        watch_events=60,
        search_events=20,
        users=30,
        contents=12,
        invalid_events=2,
        duplicate_events=1,
    )

    first = run_partition(
        spark,
        root,
        date(2025, 2, 1),
        export_dir=project / "exports",
    )
    second = run_partition(
        spark,
        root,
        date(2025, 2, 1),
        export_dir=project / "exports",
    )

    assert first["status"] == "PASS"
    assert first["watch"] == second["watch"]
    assert first["search"] == second["search"]
    assert first["gold"] == second["gold"]
    assert first["watch"]["raw"] == 61
    assert first["watch"]["deduplicated"] == 60
    assert first["watch"]["accepted"] + first["watch"]["quarantined"] == 60
    assert first["search"]["raw"] == 21
    assert all(first["assertions"].values())

    with (project / "exports" / "customer_360.csv").open(encoding="utf-8") as handle:
        assert len(list(csv.DictReader(handle))) == 30
