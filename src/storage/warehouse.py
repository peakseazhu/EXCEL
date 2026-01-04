from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import duckdb

from ..config.model import ChartConfig, TargetTableConfig
from ..utils.fs import sha256_file


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def load_schema(schema_path: Optional[str]) -> Optional[List[Dict[str, str]]]:
    if not schema_path:
        return None
    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("columns", [])


@dataclass
class Warehouse:
    path: Path

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.path))

    def init(self) -> None:
        with self.connect() as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS raw")
            con.execute("CREATE SCHEMA IF NOT EXISTS dim")
            con.execute("CREATE SCHEMA IF NOT EXISTS mart")
            con.execute("CREATE SCHEMA IF NOT EXISTS ops")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS ops.run_history (
                    run_id VARCHAR PRIMARY KEY,
                    run_date DATE,
                    status VARCHAR,
                    started_at TIMESTAMP,
                    ended_at TIMESTAMP,
                    error TEXT,
                    metrics VARCHAR
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS ops.target_versions (
                    target_name VARCHAR,
                    file_path VARCHAR,
                    sha256 VARCHAR,
                    row_count BIGINT,
                    loaded_at TIMESTAMP
                )
                """
            )

    def record_run_start(self, run_id: str, run_date: str) -> None:
        with self.connect() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO ops.run_history (run_id, run_date, status, started_at)
                VALUES (?, ?, ?, ?)
                """,
                [run_id, run_date, "running", datetime.utcnow()],
            )

    def record_run_end(self, run_id: str, status: str, error: Optional[str], metrics: Optional[dict]) -> None:
        metrics_payload = json.dumps(metrics or {}, ensure_ascii=False)
        with self.connect() as con:
            con.execute(
                """
                UPDATE ops.run_history
                SET status = ?, ended_at = ?, error = ?, metrics = ?
                WHERE run_id = ?
                """,
                [status, datetime.utcnow(), error, metrics_payload, run_id],
            )

    def raw_table_name(self, chart: ChartConfig) -> str:
        return f"raw.chart_{chart.chart_id}"

    def load_raw_csv(self, chart: ChartConfig, run_date: str, file_path: Path) -> int:
        schema = load_schema(chart.schema_path)
        table = self.raw_table_name(chart)
        path_str = str(file_path).replace("'", "''")

        with self.connect() as con:
            if schema:
                cols = ", ".join(f"{quote_ident(c['name'])} {c['type']}" for c in schema)
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} ({cols}, run_date DATE, loaded_at TIMESTAMP)"
                )
                cast_cols = ", ".join(
                    f"CAST({quote_ident(c['name'])} AS {c['type']}) AS {quote_ident(c['name'])}"
                    for c in schema
                )
                select_sql = (
                    f"SELECT {cast_cols}, DATE '{run_date}' AS run_date, CURRENT_TIMESTAMP AS loaded_at "
                    f"FROM read_csv_auto('{path_str}')"
                )
            else:
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} AS "
                    f"SELECT *, DATE '{run_date}' AS run_date, CURRENT_TIMESTAMP AS loaded_at "
                    f"FROM read_csv_auto('{path_str}') LIMIT 0"
                )
                select_sql = (
                    f"SELECT *, DATE '{run_date}' AS run_date, CURRENT_TIMESTAMP AS loaded_at "
                    f"FROM read_csv_auto('{path_str}')"
                )

            con.execute(f"DELETE FROM {table} WHERE run_date = DATE '{run_date}'")
            con.execute(f"INSERT INTO {table} {select_sql}")
            row_count = con.execute(f"SELECT COUNT(*) FROM {table} WHERE run_date = DATE '{run_date}'").fetchone()[0]
        return row_count

    def load_target_table(self, target: TargetTableConfig) -> int:
        if target.source != "file":
            raise ValueError("Only file targets are supported in this phase")
        if not target.path:
            raise ValueError("Target path is required")

        schema = load_schema(target.schema_path)
        path = Path(target.path)
        if not path.exists():
            raise FileNotFoundError(f"Target file not found: {path}")
        path_str = str(path).replace("'", "''")
        table = f"dim.{target.name}"

        with self.connect() as con:
            if schema:
                cols = ", ".join(f"{quote_ident(c['name'])} {c['type']}" for c in schema)
                con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols})")
                cast_cols = ", ".join(
                    f"CAST({quote_ident(c['name'])} AS {c['type']}) AS {quote_ident(c['name'])}"
                    for c in schema
                )
                con.execute(f"DELETE FROM {table}")
                con.execute(
                    f"INSERT INTO {table} SELECT {cast_cols} FROM read_csv_auto('{path_str}')"
                )
            else:
                con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv_auto('{path_str}')")

            row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            con.execute(
                "INSERT INTO ops.target_versions VALUES (?, ?, ?, ?, ?)",
                [target.name, str(path), sha256_file(path), row_count, datetime.utcnow()],
            )

        return row_count