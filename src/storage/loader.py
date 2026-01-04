from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from ..config.model import PipelineConfig
from ..core.context import RunContext
from .warehouse import Warehouse


@dataclass
class LoadResult:
    raw_rows: Dict[str, int]
    target_rows: Dict[str, int]


def run_load(config: PipelineConfig, context: RunContext, logger) -> LoadResult:
    warehouse = Warehouse(context.warehouse_path)
    warehouse.init()

    raw_rows: Dict[str, int] = {}
    for chart in config.bi.charts:
        csv_path = context.raw_dir / f"chart_id={chart.chart_id}" / "data.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing raw CSV for chart {chart.chart_id}: {csv_path}")
        rows = warehouse.load_raw_csv(chart, context.run_date.strftime("%Y-%m-%d"), csv_path)
        raw_rows[chart.chart_id] = rows
        logger.info("Loaded raw %s rows for %s", rows, chart.chart_id)

    target_rows: Dict[str, int] = {}
    for target in config.targets.tables:
        rows = warehouse.load_target_table(target)
        target_rows[target.name] = rows
        logger.info("Loaded target %s rows for %s", rows, target.name)

    return LoadResult(raw_rows=raw_rows, target_rows=target_rows)