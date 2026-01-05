# EXCEL (BI Pipeline)

This project is refactored into a DuckDB-based data pipeline that **does not depend on Excel**. It exports data from Guanbi BI, stores raw files, transforms with SQL, and publishes results to Feishu Sheets.

## Quick Start

```bash
python -m src --config config/config.json validate-config
```

## Common Commands

```bash
python -m src --config config/config.json extract --date 2025-01-01
python -m src --config config/config.json load --date 2025-01-01
python -m src --config config/config.json transform --date 2025-01-01
python -m src --config config/config.json publish --date 2025-01-01
python -m src --config config/config.json run --date 2025-01-01
python -m src --config config/config.json backfill --start 2025-01-01 --end 2025-01-07
```

## Directory Layout

- `config/` : pipeline config and target tables (weekly full refresh)
- `schemas/` : optional schema definitions for raw/target tables
- `sql/mart/` : SQL for result tables
- `data/` : raw data, manifests, reports, DuckDB warehouse (runtime)
- `baseline/` : baseline CSVs for compare command
- `docs/` : docs and runbooks
- `scripts/` : scheduling templates

## Docs

- `docs/architecture.md`
- `docs/runbook.md`

## Notes

- Replace `config/targets/targets_a.csv` and `targets_b.csv` weekly with full data.
- Configure BI and Feishu credentials via environment variables.
- For charts that only support table export, set `export_fallbacks` to include `"complex"` in `config/config.json`.
