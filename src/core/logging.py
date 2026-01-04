from __future__ import annotations

import logging
from pathlib import Path

from ..utils.fs import ensure_dir


def setup_logging(log_dir: Path, run_id: str) -> logging.Logger:
    ensure_dir(log_dir)
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        file_handler = logging.FileHandler(log_dir / f"run_{run_id}.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger