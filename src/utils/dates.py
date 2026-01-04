from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def today(tz_name: str) -> date:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz=tz).date()


def yesterday(tz_name: str) -> date:
    return today(tz_name) - timedelta(days=1)


def to_datestr(value: date) -> str:
    return value.strftime("%Y-%m-%d")