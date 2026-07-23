# Runbook

## Normal operation

Run an incremental partition with:

```bash
customer360 run --root data --start-date 2025-02-01 --days 1 --mode incremental
```

Backfill a bounded date range by changing `--days`. The pipeline overwrites the same
date partition and rebuilds gold snapshots, making replay idempotent.

## Recovery

If a run stops during silver/gold writes, rerun the same command. Spark writes through
temporary files and the deterministic partition replacement prevents partial rows from
being appended. Inspect `artifacts/quality-report.json` before publishing exports.

## Reset

`customer360 demo` removes only this project's `data/`, `artifacts/`, and generated
Power BI exports. `docker compose down` removes containers; generated bind-mounted data
remains on disk.

## Data-quality triage

Rejected rows are stored under `data/lakehouse/silver/*_rejected/`. Group by
`rejection_reason`, correct the source or contract, then replay the affected date.
