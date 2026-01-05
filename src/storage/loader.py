from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from ..config.model import PipelineConfig, TargetTableConfig
from ..core.context import RunContext
from .warehouse import Warehouse
from ..utils.convert import xlsx_to_csv
from ..utils.fs import ensure_dir


@dataclass
class LoadResult:
    raw_rows: Dict[str, int]
    target_rows: Dict[str, int]


def _resolve_target_paths(
    target: TargetTableConfig,
    context: RunContext,
    logger,
) -> tuple[Path, Path]:
    if not target.path:
        raise ValueError("Target path is required")
    source_path = Path(target.path)
    suffix = source_path.suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        cache_dir = ensure_dir(context.data_dir / "targets_cache")
        output_path = cache_dir / f"{target.name}.csv"
        xlsx_to_csv(source_path, output_path, target.sheet_name)
        logger.info("Converted target %s from %s to %s", target.name, source_path, output_path)
        return source_path, output_path
    if suffix == ".xls":
        raise ValueError(f"Unsupported target file type (.xls) for {source_path}")
    return source_path, source_path


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
        source_path, resolved_path = _resolve_target_paths(target, context, logger)
        rows = warehouse.load_target_table(target, resolved_path=resolved_path, source_path=source_path)
        target_rows[target.name] = rows
        logger.info("Loaded target %s rows for %s", rows, target.name)

    return LoadResult(raw_rows=raw_rows, target_rows=target_rows)
