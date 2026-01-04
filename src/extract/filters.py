from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from typing import List, Dict

from ..utils.dates import to_datestr
from ..config.model import FilterRules


def apply_filter_rules(filters: List[Dict], rules: FilterRules, run_date: date) -> Dict[str, List[Dict]]:
    updated = deepcopy(filters)

    if rules.update_date:
        start_date = run_date - timedelta(days=max(rules.days_ago_start - 1, 0))
        end_date = run_date - timedelta(days=max(rules.days_ago_end - 1, 0))
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        date_range = [to_datestr(start_date), to_datestr(end_date)]
        for item in updated:
            if item.get("filterType") == "BT":
                item["filterValue"] = date_range

    if rules.update_month:
        month_value = rules.month or run_date.strftime("%Y-%m")
        for item in updated:
            if item.get("name") == "\u6708\u4efd":
                item["filterValue"] = [month_value]

    return {"filters": updated}