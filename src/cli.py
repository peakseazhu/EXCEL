from __future__ import annotations

import argparse
from pathlib import Path

from .config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BI data pipeline")
    parser.add_argument("--config", default="config/config.json")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("validate-config", help="Validate config file")

    for name in ("run", "extract", "load", "transform", "publish", "profile", "compare", "backfill"):
        sub = subparsers.add_parser(name, help=f"{name} command")
        if name in {"run", "extract", "load", "transform", "publish", "profile", "compare"}:
            sub.add_argument("--date", help="Run date (YYYY-MM-DD)")
        if name == "backfill":
            sub.add_argument("--start", required=True)
            sub.add_argument("--end", required=True)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "validate-config":
        config = load_config(Path(args.config))
        print(f"Config OK: {config.project.name}")
        return

    raise SystemExit("Command not implemented yet. Run in later phases after full refactor.")


if __name__ == "__main__":
    main()