# EXCEL (BI Pipeline)

This project is refactored into a DuckDB-based data pipeline that **does not depend on Excel**. It exports data from Guanbi BI, stores raw files, transforms with SQL, and publishes results to Feishu Sheets.

## Quick Start

```bash
python -m src --config config/config.json validate-config
```

## Directory Layout

- `config/` : pipeline config and target tables (weekly full refresh)
- `schemas/` : optional schema definitions for raw/target tables
- `sql/mart/` : SQL for result tables
- `data/` : raw data, manifests, reports, DuckDB warehouse (runtime)
- `docs/` : docs and runbooks
- `scripts/` : scheduling templates

## Notes

- Replace `config/targets/targets_a.csv` and `targets_b.csv` weekly with full data.
- Configure BI and Feishu credentials via environment variables.
