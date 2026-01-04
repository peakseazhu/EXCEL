from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb

from ..config.model import PipelineConfig
from ..core.context import RunContext
from ..utils.fs import write_json
from ..storage.warehouse import quote_ident


NUMERIC_TYPES = {
    "INTEGER",
    "BIGINT",
    "DOUBLE",
    "REAL",
    "DECIMAL",
    "HUGEINT",
    "UBIGINT",
    "SMALLINT",
    "TINYINT",
}


def _baseline_path(baseline_dir: Path, table: str) -> Path:
    return baseline_dir / f"{table}.csv"


def _fetch_records(con: duckdb.DuckDBPyConnection, query: str, params: List) -> List[dict]:
    cursor = con.execute(query, params)
    cols = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def _select_with_rounding(con: duckdb.DuckDBPyConnection, table: str, rounding: int) -> str:
    columns = con.execute(f"DESCRIBE {table}").fetchall()
    selects: List[str] = []
    for name, col_type, *_ in columns:
        col_type_upper = col_type.upper()
        if any(col_type_upper.startswith(t) for t in NUMERIC_TYPES):
            selects.append(f"ROUND({quote_ident(name)}, {rounding}) AS {quote_ident(name)}")
        else:
            selects.append(f"{quote_ident(name)}")
    return ", ".join(selects)


@dataclass
class CompareResult:
    table_stats: Dict[str, dict]
    report_path: Path


def run_compare(config: PipelineConfig, context: RunContext, logger) -> CompareResult:
    baseline_dir = Path(config.compare.baseline_dir)
    report = {
        "run_id": context.run_id,
        "run_date": context.run_date.strftime("%Y-%m-%d"),
        "tables": [],
    }

    with duckdb.connect(str(context.warehouse_path)) as con:
        for output in config.feishu.outputs:
            table = output.table
            baseline_path = _baseline_path(baseline_dir, table)
            entry = {
                "table": table,
                "baseline_path": str(baseline_path),
                "baseline_exists": baseline_path.exists(),
            }
            if not baseline_path.exists():
                report["tables"].append(entry)
                continue

            select_curr = _select_with_rounding(con, table, config.compare.rounding)
            select_base = select_curr

            con.execute("DROP VIEW IF EXISTS curr")
            con.execute("DROP VIEW IF EXISTS base")
            con.execute(
                f"CREATE TEMP VIEW curr AS SELECT {select_curr} FROM {table}"
            )
            con.execute(
                "CREATE TEMP VIEW base AS SELECT * FROM read_csv_auto(?)",
                [str(baseline_path)],
            )

            missing = con.execute("SELECT COUNT(*) FROM (SELECT * FROM base EXCEPT SELECT * FROM curr)").fetchone()[0]
            extra = con.execute("SELECT COUNT(*) FROM (SELECT * FROM curr EXCEPT SELECT * FROM base)").fetchone()[0]
            entry["missing_rows"] = missing
            entry["extra_rows"] = extra

            sample_missing = _fetch_records(
                con,
                "SELECT * FROM base EXCEPT SELECT * FROM curr LIMIT ?",
                [config.compare.max_samples],
            )
            sample_extra = _fetch_records(
                con,
                "SELECT * FROM curr EXCEPT SELECT * FROM base LIMIT ?",
                [config.compare.max_samples],
            )
            entry["sample_missing"] = sample_missing
            entry["sample_extra"] = sample_extra

            report["tables"].append(entry)
            logger.info("Compare %s missing=%s extra=%s", table, missing, extra)

    report_path = context.report_dir / f"compare_{context.run_date.strftime('%Y-%m-%d')}.json"
    write_json(report_path, report)
    return CompareResult(table_stats={t["table"]: t for t in report["tables"]}, report_path=report_path)
