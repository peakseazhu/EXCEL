from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..config.model import ChartConfig
from ..core.context import RunContext
from ..utils.fs import ensure_dir, sha256_file


def save_raw_bytes(context: RunContext, chart: ChartConfig, content: bytes, extension: str) -> Path:
    chart_dir = ensure_dir(context.raw_dir / f"chart_id={chart.chart_id}")
    filename = f"data{extension}"
    path = chart_dir / filename
    path.write_bytes(content)
    return path


def count_csv_rows(path: Path) -> int:
    line_count = 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for _ in handle:
            line_count += 1
    if line_count == 0:
        return 0
    return max(line_count - 1, 0)


def build_export_record(
    chart: ChartConfig,
    file_path: Path,
    csv_path: Path,
    filters: dict,
    row_count: Optional[int],
    export_format: str,
    export_mode: str,
) -> dict:
    return {
        "chart_id": chart.chart_id,
        "chart_name": chart.name,
        "export_format": export_format,
        "export_mode": export_mode,
        "file_path": str(file_path),
        "csv_path": str(csv_path),
        "file_size": file_path.stat().st_size,
        "sha256": sha256_file(file_path),
        "filters": filters,
        "row_count": row_count,
    }
