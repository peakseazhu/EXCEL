from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from uuid import uuid4

from ..utils.dates import to_datestr


@dataclass(frozen=True)
class RunContext:
    run_date: date
    data_dir: Path
    log_dir: Path
    run_id: str

    @classmethod
    def create(cls, run_date: date, data_dir: Path, log_dir: Path) -> "RunContext":
        run_id = f"{to_datestr(run_date)}-{uuid4().hex[:8]}"
        return cls(run_date=run_date, data_dir=data_dir, log_dir=log_dir, run_id=run_id)

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw" / f"run_date={to_datestr(self.run_date)}"

    @property
    def manifest_dir(self) -> Path:
        return self.data_dir / "manifests"

    @property
    def report_dir(self) -> Path:
        return self.data_dir / "reports"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"