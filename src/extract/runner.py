from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from ..config import get_env_or_fail
from ..config.model import PipelineConfig, ChartConfig
from ..core.context import RunContext
from ..extract.filters import apply_filter_rules
from ..extract.guanbi import GuanbiClient
from ..utils.convert import xlsx_to_csv
from ..storage import ManifestWriter, save_raw_bytes, count_csv_rows, build_export_record
from ..utils.fs import ensure_dir


@dataclass
class ExtractResult:
    files: Dict[str, Path]
    row_counts: Dict[str, int | None]


def _normalize_format(value: str) -> str:
    value = value.lower()
    if value == "excel":
        return "xlsx"
    if value in {"table", "pivot"}:
        return "pivot"
    return value


def _build_attempts(chart: ChartConfig, default_format: str) -> List[Tuple[str, str]]:
    if chart.export_fallbacks:
        raw_attempts = chart.export_fallbacks
    else:
        raw_attempts = [chart.export_format or default_format, "xlsx", "pivot", "complex"]
        if chart.export_mode == "complex":
            raw_attempts = ["complex", chart.export_format or default_format, "xlsx", "pivot"]
    attempts: List[Tuple[str, str]] = []
    for raw in raw_attempts:
        if not raw:
            continue
        normalized = _normalize_format(raw)
        if normalized in {"csv", "xlsx", "pivot"}:
            attempts.append(("simple", normalized))
        elif normalized in {"complex"}:
            attempts.append(("complex", "xlsx"))
    seen = set()
    unique: List[Tuple[str, str]] = []
    for attempt in attempts:
        if attempt not in seen:
            unique.append(attempt)
            seen.add(attempt)
    return unique


def run_extract(config: PipelineConfig, context: RunContext, logger) -> ExtractResult:
    ensure_dir(context.raw_dir)
    manifest = ManifestWriter(context)

    username = get_env_or_fail(config.bi.username_env)
    password = get_env_or_fail(config.bi.password_env)

    client = GuanbiClient(
        config=config.bi,
        username=username,
        password=password,
        timeout_seconds=config.project.request_timeout_seconds,
        max_retries=config.project.request_max_retries,
        poll_interval_seconds=config.project.task_poll_interval_seconds,
        max_wait_seconds=config.project.task_max_wait_seconds,
        logger=logger,
    )

    token = client.sign_in()
    logger.info("Signed in to Guanbi")

    files: Dict[str, Path] = {}
    row_counts: Dict[str, int | None] = {}

    for chart in config.bi.charts:
        filters = apply_filter_rules(chart.filters, chart.filter_rules, context.run_date)
        attempts = _build_attempts(chart, config.project.export_format)
        if not attempts:
            raise ValueError(f"No export attempts configured for chart {chart.chart_id}")

        last_error: Exception | None = None
        for mode, export_format in attempts:
            try:
                task_id, file_name = client.create_task(chart.chart_id, token, filters, mode, export_format)
                logger.info("Created task %s for chart %s (%s/%s)", task_id, chart.chart_id, mode, export_format)

                finished_time = client.poll_task(task_id, token)
                logger.info("Task %s finished", task_id)

                content = client.download(token, file_name, finished_time, mode, export_format)
                extension = ".csv" if export_format == "csv" and mode == "simple" else ".xlsx"
                file_path = save_raw_bytes(context, chart, content, extension)

                if extension == ".csv":
                    csv_path = file_path
                else:
                    csv_path = file_path.with_suffix(".csv")
                    xlsx_to_csv(file_path, csv_path, chart.sheet_name)

                row_count = count_csv_rows(csv_path)
                record = build_export_record(chart, file_path, csv_path, filters, row_count, export_format, mode)
                manifest.add_export(record)

                files[chart.chart_id] = csv_path
                row_counts[chart.chart_id] = row_count
                logger.info("Saved export for %s to %s (csv %s)", chart.chart_id, file_path, csv_path)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Export attempt failed for %s (%s/%s): %s",
                    chart.chart_id,
                    mode,
                    export_format,
                    exc,
                )
                continue

        if last_error is not None:
            raise last_error

    return ExtractResult(files=files, row_counts=row_counts)
