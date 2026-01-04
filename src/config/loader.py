from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from .model import PipelineConfig


class ConfigError(RuntimeError):
    pass


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise ConfigError("PyYAML is required to load YAML config") from exc
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_config(path: str | Path) -> PipelineConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")
    if config_path.suffix.lower() in {".yaml", ".yml"}:
        raw = _load_yaml(config_path)
    elif config_path.suffix.lower() == ".json":
        raw = _load_json(config_path)
    else:
        raise ConfigError("Config must be .json or .yaml")
    return PipelineConfig.model_validate(raw)


def get_env_or_fail(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value