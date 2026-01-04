from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from ..config import get_env_or_fail
from ..config.model import PipelineConfig, ChartConfig
from ..core.context import RunContext
from ..extract.filters import apply_filter_rules
from ..extract.guanbi import GuanbiClient
from ..storage import ManifestWriter, save_raw_bytes, count_csv_rows, build_export_record
from ..utils.fs import ensure_dir


@dataclass
class ExtractResult:
    files: Dict[str, Path]
    row_counts: Dict[str, int | None]


def _extension_for(chart: ChartConfig, default_format: str) -> str:
    export_format = chart.export_format or default_format
    return ".csv" if export_format == "csv" else ".xlsx"


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
        task_id, file_name = client.create_task(chart, token, filters)
        logger.info("Created task %s for chart %s", task_id, chart.chart_id)

        finished_time = client.poll_task(task_id, token)
        logger.info("Task %s finished", task_id)

        content = client.download(chart, token, file_name, finished_time)
        extension = _extension_for(chart, config.project.export_format)
        file_path = save_raw_bytes(context, chart, content, extension)

        row_count = None
        if extension == ".csv":
            row_count = count_csv_rows(file_path)
        record = build_export_record(chart, file_path, filters, row_count)
        manifest.add_export(record)

        files[chart.chart_id] = file_path
        row_counts[chart.chart_id] = row_count
        logger.info("Saved export for %s to %s", chart.chart_id, file_path)

    return ExtractResult(files=files, row_counts=row_counts)
