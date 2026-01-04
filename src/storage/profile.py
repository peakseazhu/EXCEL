from __future__ import annotations

from pathlib import Path
from typing import List

import duckdb

from ..config.model import ChartConfig
from ..core.context import RunContext
from ..utils.fs import write_json


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def profile_raw_files(context: RunContext, charts: List[ChartConfig], sample_rows: int) -> Path:
    report = {
        "run_id": context.run_id,
        "run_date": context.run_date.strftime("%Y-%m-%d"),
        "charts": [],
    }
    for chart in charts:
        csv_path = context.raw_dir / f"chart_id={chart.chart_id}" / "data.csv"
        entry = {
            "chart_id": chart.chart_id,
            "chart_name": chart.name,
            "file_path": str(csv_path),
            "exists": csv_path.exists(),
        }
        if not csv_path.exists():
            report["charts"].append(entry)
            continue

        con = duckdb.connect()
        con.execute("CREATE VIEW raw AS SELECT * FROM read_csv_auto(?)", [str(csv_path)])
        row_count = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
        entry["row_count"] = row_count
        entry["sample_rows"] = min(sample_rows, row_count) if sample_rows else row_count

        columns = con.execute("DESCRIBE raw").fetchall()
        column_stats = []
        sample_query = "SELECT * FROM raw"
        if sample_rows:
            sample_query = f"SELECT * FROM raw LIMIT {sample_rows}"

        for name, col_type, *_ in columns:
            quoted = quote_ident(name)
            stats = con.execute(
                f"SELECT COUNT(*) AS rows, COUNT(DISTINCT {quoted}) AS distinct_count, "
                f"SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS null_count "
                f"FROM ({sample_query})"
            ).fetchone()
            column_stats.append({
                "name": name,
                "type": col_type,
                "sample_rows": stats[0],
                "sample_distinct": stats[1],
                "sample_nulls": stats[2],
            })
        entry["columns"] = column_stats
        report["charts"].append(entry)

    report_path = context.report_dir / f"profile_{context.run_date.strftime('%Y-%m-%d')}.json"
    write_json(report_path, report)
    return report_path