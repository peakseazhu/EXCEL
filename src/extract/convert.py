from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

import openpyxl


def xlsx_to_csv(source_path: Path, output_path: Path, sheet_name: Optional[str] = None) -> None:
    wb = openpyxl.load_workbook(source_path, read_only=True, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb.worksheets[0]

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        for row in ws.iter_rows(values_only=True):
            writer.writerow(list(row))

    wb.close()