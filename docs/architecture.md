# Architecture

## Flow

1. Extract CSV from Guanbi BI into `data/raw/run_date=YYYY-MM-DD/...`.
2. Load raw data + weekly target tables into DuckDB.
3. Transform raw/dim tables into `mart.*` using SQL files in `sql/mart/`.
4. Publish results to Feishu Sheets.
5. Record run history and publish history in DuckDB (`ops.*`).

## Key Tables

- `raw.chart_<chart_id>`: raw exports with `run_date` and `loaded_at`.
- `dim.targets_a` / `dim.targets_b`: weekly full refresh targets.
- `mart.*`: result tables for Feishu outputs.
- `ops.run_history`: run status and metrics.
- `ops.publish_history`: last published row/column counts for clearing tail.