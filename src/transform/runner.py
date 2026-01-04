from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import duckdb

from ..core.context import RunContext


def render_sql(sql_text: str, run_date: str) -> str:
    return sql_text.replace("{{ run_date }}", run_date)


@dataclass
class TransformResult:
    table_rows: Dict[str, int]


def run_transform(context: RunContext, sql_dir: Path, logger) -> TransformResult:
    run_date = context.run_date.strftime("%Y-%m-%d")
    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in {sql_dir}")

    table_rows: Dict[str, int] = {}
    with duckdb.connect(str(context.warehouse_path)) as con:
        for sql_file in sql_files:
            sql_text = sql_file.read_text(encoding="utf-8")
            rendered = render_sql(sql_text, run_date)
            con.execute(rendered)
            table_name = f"mart.{sql_file.stem}"
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            table_rows[table_name] = row_count
            logger.info("Transformed %s rows into %s", row_count, table_name)
    return TransformResult(table_rows=table_rows)