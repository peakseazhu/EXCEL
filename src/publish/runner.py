from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import duckdb

from ..config import get_env_or_fail
from ..config.model import PipelineConfig, OutputSheetConfig
from ..core.context import RunContext
from ..publish.feishu import FeishuClient, get_excel_range
from ..storage import Warehouse


CELL_RE = re.compile(r"^([A-Za-z]+)(\d+)$")


def parse_cell(cell: str) -> Tuple[int, int]:
    match = CELL_RE.match(cell)
    if not match:
        raise ValueError(f"Invalid cell format: {cell}")
    col_letters, row = match.groups()
    col = 0
    for char in col_letters.upper():
        col = col * 26 + (ord(char) - ord("A") + 1)
    return col, int(row)


def _fetch_batches(cursor: duckdb.DuckDBPyConnection, batch_size: int) -> Iterable[List[List]]:
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield [list(row) for row in rows]


@dataclass
class PublishResult:
    sheet_rows: Dict[str, int]


def _write_batch(client: FeishuClient, spreadsheet_token: str, sheet_id: str, start_row: int, start_col: int, values: List[List]) -> None:
    if not values:
        return
    rows = len(values)
    cols = len(values[0]) if values[0] else 0
    range_str = f"{sheet_id}!{get_excel_range(start_row, start_col, rows, cols)}"
    client.write_values(spreadsheet_token, range_str, values)


def _clear_tail(client: FeishuClient, spreadsheet_token: str, sheet_id: str, start_row: int, start_col: int, rows: int, cols: int, batch_size: int) -> None:
    remaining = rows
    current_row = start_row
    blank_row = ["" for _ in range(cols)]
    while remaining > 0:
        take = min(batch_size, remaining)
        values = [blank_row for _ in range(take)]
        range_str = f"{sheet_id}!{get_excel_range(current_row, start_col, take, cols)}"
        client.write_values(spreadsheet_token, range_str, values)
        current_row += take
        remaining -= take


def run_publish(config: PipelineConfig, context: RunContext, logger) -> PublishResult:
    app_id = get_env_or_fail(config.feishu.app_id_env)
    app_secret = get_env_or_fail(config.feishu.app_secret_env)
    client = FeishuClient(
        app_id=app_id,
        app_secret=app_secret,
        timeout_seconds=config.project.request_timeout_seconds,
        max_retries=config.project.request_max_retries,
        logger=logger,
    )

    warehouse = Warehouse(context.warehouse_path)
    warehouse.init()

    sheets = client.list_sheets(config.feishu.spreadsheet_token)
    sheet_cache: Dict[str, str] = {s.get("title"): s.get("sheet_id") for s in sheets}

    sheet_rows: Dict[str, int] = {}

    with duckdb.connect(str(context.warehouse_path)) as con:
        for output in config.feishu.outputs:
            table = output.table
            sheet_id = sheet_cache.get(output.sheet_name)
            if not sheet_id:
                raise ValueError(f"Sheet not found: {output.sheet_name}")
            start_col, start_row = parse_cell(output.start_cell)

            prev_publish = warehouse.get_last_publish(output.sheet_name)
            cursor = con.execute(f"SELECT * FROM {table}")
            columns = [desc[0] for desc in cursor.description]

            current_row = start_row
            total_rows = 0
            if output.include_header:
                _write_batch(client, config.feishu.spreadsheet_token, sheet_id, current_row, start_col, [columns])
                current_row += 1
                total_rows += 1

            for batch in _fetch_batches(cursor, output.batch_size):
                _write_batch(client, config.feishu.spreadsheet_token, sheet_id, current_row, start_col, batch)
                current_row += len(batch)
                total_rows += len(batch)

            sheet_rows[output.sheet_name] = total_rows
            if output.clear_extra_rows and prev_publish:
                prev_rows, prev_cols = prev_publish
                extra_rows = max(prev_rows - total_rows, 0)
                if extra_rows > 0:
                    _clear_tail(
                        client,
                        config.feishu.spreadsheet_token,
                        sheet_id,
                        start_row + total_rows,
                        start_col,
                        extra_rows,
                        prev_cols,
                        output.batch_size,
                    )
                    logger.info("Cleared %s extra rows for %s", extra_rows, output.sheet_name)

            warehouse.record_publish(
                context.run_id,
                context.run_date.strftime("%Y-%m-%d"),
                output.sheet_name,
                total_rows,
                len(columns),
            )

            logger.info("Published %s rows to %s", total_rows, output.sheet_name)

    return PublishResult(sheet_rows=sheet_rows)
