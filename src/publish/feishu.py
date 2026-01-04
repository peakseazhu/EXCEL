from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

from ..utils.retry import create_retry_session


def col_num_to_letter(n: int) -> str:
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def get_excel_range(start_row: int, start_col: int, rows: int, cols: int) -> str:
    end_row = start_row + rows - 1
    end_col = start_col + cols - 1
    start_cell = f"{col_num_to_letter(start_col)}{start_row}"
    end_cell = f"{col_num_to_letter(end_col)}{end_row}"
    return f"{start_cell}:{end_cell}"


@dataclass
class FeishuClient:
    app_id: str
    app_secret: str
    timeout_seconds: int
    max_retries: int
    logger: any

    def __post_init__(self) -> None:
        self.session = create_retry_session(self.max_retries)
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0

    def _get_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expiry:
            return self._token

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        response = self.session.post(url, json=payload, timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to get token: {data}")
        token = data.get("tenant_access_token")
        expire = data.get("expire", 3600)
        self._token = token
        self._token_expiry = now + expire - 60
        return token

    def _auth_headers(self) -> Dict[str, str]:
        token = self._get_token()
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}

    def list_sheets(self, spreadsheet_token: str) -> List[Dict]:
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
        response = self.session.get(url, headers=self._auth_headers(), timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to list sheets: {data}")
        return data.get("data", {}).get("sheets", [])

    def get_sheet_id(self, spreadsheet_token: str, sheet_name: str) -> str:
        sheets = self.list_sheets(spreadsheet_token)
        for sheet in sheets:
            if sheet.get("title") == sheet_name:
                return sheet.get("sheet_id")
        raise ValueError(f"Sheet not found: {sheet_name}")

    def write_values(self, spreadsheet_token: str, range_str: str, values: List[List]) -> Dict:
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
        payload = {"valueRange": {"range": range_str, "values": values}}
        response = self.session.put(url, headers=self._auth_headers(), json=payload, timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to write values: {data}")
        return data

    def send_alert(self, receive_id_type: str, receive_id: str, content: str) -> None:
        url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
        payload = {
            "receive_id": receive_id,
            "msg_type": "text",
            "content": {"text": content},
        }
        response = self.session.post(url, headers=self._auth_headers(), json=payload, timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to send alert: {data}")