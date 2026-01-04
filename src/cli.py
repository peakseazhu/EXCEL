from __future__ import annotations

import argparse
from pathlib import Path

from .config import load_config
from .core import RunContext, setup_logging
from .extract import run_extract
from .storage import profile_raw_files, run_load
from .utils.dates import parse_date, yesterday


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


def _resolve_run_date(date_arg: str | None, timezone: str):
    if date_arg:
        return parse_date(date_arg)
    return yesterday(timezone)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = load_config(Path(args.config))
    if args.command == "validate-config":
        print(f"Config OK: {config.project.name}")
        return

    run_date = _resolve_run_date(getattr(args, "date", None), config.project.timezone)
    context = RunContext.create(run_date, Path(config.project.data_dir), Path(config.project.log_dir))
    logger = setup_logging(context.log_dir, context.run_id)

    if args.command == "extract":
        run_extract(config, context, logger)
        logger.info("Extract completed: %s", context.run_id)
        return

    if args.command == "profile":
        report_path = profile_raw_files(context, config.bi.charts, config.project.profile_sample_rows)
        logger.info("Profile report written: %s", report_path)
        return

    if args.command == "load":
        run_load(config, context, logger)
        logger.info("Load completed: %s", context.run_id)
        return

    raise SystemExit("Command not implemented yet. Run in later phases after full refactor.")


if __name__ == "__main__":
    main()
