from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from .config import load_config
from .core import RunContext, setup_logging
from .extract import run_extract
from .storage import profile_raw_files, run_load
from .transform import run_transform, run_compare
from .publish import run_publish
from .pipeline import run_pipeline
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

    load_dotenv()
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

    if args.command == "transform":
        sql_dir = Path("sql/mart")
        run_transform(context, sql_dir, logger)
        logger.info("Transform completed: %s", context.run_id)
        return

    if args.command == "compare":
        result = run_compare(config, context, logger)
        logger.info("Compare report written: %s", result.report_path)
        return

    if args.command == "publish":
        run_publish(config, context, logger)
        logger.info("Publish completed: %s", context.run_id)
        return

    if args.command == "run":
        run_pipeline(config, context, logger)
        return

    if args.command == "backfill":
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
        if start_date > end_date:
            raise SystemExit("backfill start date must be <= end date")
        failures = []
        current = start_date
        while current <= end_date:
            bf_context = RunContext.create(current, Path(config.project.data_dir), Path(config.project.log_dir))
            bf_logger = setup_logging(bf_context.log_dir, bf_context.run_id)
            try:
                run_pipeline(config, bf_context, bf_logger)
            except Exception as exc:
                failures.append((current.strftime("%Y-%m-%d"), str(exc)))
            current = current.fromordinal(current.toordinal() + 1)
        if failures:
            raise SystemExit(f"Backfill completed with failures: {failures}")
        return

    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
