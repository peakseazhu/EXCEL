from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from .config.model import PipelineConfig
from .core.context import RunContext
from .extract import run_extract
from .storage import Warehouse, run_load
from .transform import run_transform
from .publish import run_publish, FeishuClient
from .config import get_env_or_fail


@dataclass
class PipelineResult:
    metrics: Dict


def run_pipeline(config: PipelineConfig, context: RunContext, logger) -> PipelineResult:
    warehouse = Warehouse(context.warehouse_path)
    warehouse.init()
    run_date = context.run_date.strftime("%Y-%m-%d")
    warehouse.record_run_start(context.run_id, run_date)

    metrics: Dict[str, dict] = {}
    try:
        start = time.time()
        extract_result = run_extract(config, context, logger)
        metrics["extract"] = {
            "seconds": time.time() - start,
            "row_counts": extract_result.row_counts,
        }

        start = time.time()
        load_result = run_load(config, context, logger)
        metrics["load"] = {
            "seconds": time.time() - start,
            "raw_rows": load_result.raw_rows,
            "target_rows": load_result.target_rows,
        }

        start = time.time()
        transform_result = run_transform(context, Path("sql/mart"), logger)
        metrics["transform"] = {
            "seconds": time.time() - start,
            "table_rows": transform_result.table_rows,
        }

        start = time.time()
        publish_result = run_publish(config, context, logger)
        metrics["publish"] = {
            "seconds": time.time() - start,
            "sheet_rows": publish_result.sheet_rows,
        }

        warehouse.record_run_end(context.run_id, "success", None, metrics)
        logger.info("Pipeline completed: %s", context.run_id)
        return PipelineResult(metrics=metrics)
    except Exception as exc:
        warehouse.record_run_end(context.run_id, "failed", str(exc), metrics)
        if config.feishu.alert:
            try:
                client = FeishuClient(
                    app_id=get_env_or_fail(config.feishu.app_id_env),
                    app_secret=get_env_or_fail(config.feishu.app_secret_env),
                    timeout_seconds=config.project.request_timeout_seconds,
                    max_retries=config.project.request_max_retries,
                    logger=logger,
                )
                content = f"Pipeline failed: run_id={context.run_id} error={exc}"
                client.send_alert(
                    config.feishu.alert.receive_id_type,
                    config.feishu.alert.receive_id,
                    content,
                )
            except Exception as alert_exc:
                logger.error("Failed to send alert: %s", alert_exc)
        raise
