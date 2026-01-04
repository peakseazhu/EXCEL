from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from ..core.context import RunContext
from ..utils.fs import write_json


@dataclass
class ManifestWriter:
    context: RunContext
    exports: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def manifest_path(self) -> Path:
        return self.context.manifest_dir / f"{self.context.run_id}.json"

    def add_export(self, entry: Dict[str, Any]) -> None:
        self.exports.append(entry)
        self.save()

    def save(self) -> None:
        payload = {
            "run_id": self.context.run_id,
            "run_date": self.context.run_date.strftime("%Y-%m-%d"),
            "updated_at": datetime.utcnow().isoformat(),
            "exports": self.exports,
        }
        write_json(self.manifest_path, payload)