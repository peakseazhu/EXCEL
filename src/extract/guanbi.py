from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Tuple

from ..config.model import BIConfig
from ..utils.retry import create_retry_session


TYPE_OP = {
    "csv": "CSV",
    "xlsx": "EXCEL",
}

DOWNLOAD_PATH = {
    "csv": "/api/export/file/csv/{task_filename}",
    "xlsx": "/api/export/file/excel/{task_filename}",
}


@dataclass
class GuanbiClient:
    config: BIConfig
    username: str
    password: str
    timeout_seconds: int
    max_retries: int
    poll_interval_seconds: int
    max_wait_seconds: int
    logger: any

    def __post_init__(self) -> None:
        self.session = create_retry_session(self.max_retries)

    @property
    def base_url(self) -> str:
        return self.config.base_url.rstrip("/")

    def sign_in(self) -> str:
        url = f"{self.base_url}/api/user/sign-in"
        payload = {
            "domain": self.config.domain,
            "loginId": self.username,
            "password": self.password,
        }
        response = self.session.post(url, json=payload, timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        token = data.get("uIdToken")
        if not token:
            raise RuntimeError(f"Missing uIdToken in response: {data}")
        return token

    def _headers(self, token: str) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Cookie": f"uIdToken={token}",
            "token": token,
        }

    def create_task(self, chart_id: str, token: str, filters: Dict, mode: str, export_format: str) -> Tuple[str, str]:
        if mode == "complex":
            url = f"{self.base_url}/api/complex-report/{chart_id}/generate"
        else:
            type_op = TYPE_OP.get(export_format)
            if not type_op:
                raise ValueError(f"Unsupported export format: {export_format}")
            url = f"{self.base_url}/api/write/file/{chart_id}?typeOp={type_op}"
        response = self.session.post(url, json=filters, headers=self._headers(token), timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        task_id = data.get("taskId")
        file_name = data.get("fileName")
        if not task_id or not file_name:
            raise RuntimeError(f"Unexpected task response: {data}")
        return task_id, file_name

    def poll_task(self, task_id: str, token: str) -> str:
        url = f"{self.base_url}/api/task/{task_id}"
        start = time.time()
        while True:
            response = self.session.get(url, headers=self._headers(token), timeout=self.timeout_seconds)
            response.raise_for_status()
            data = response.json()
            status = data.get("status")
            if status == "FINISHED":
                finished = data.get("finishedTime")
                if not finished:
                    raise RuntimeError(f"Missing finishedTime: {data}")
                return finished
            if status in {"FAILED", "CANCELLED"}:
                raise RuntimeError(f"Task {task_id} failed: {data}")
            if time.time() - start > self.max_wait_seconds:
                raise TimeoutError(f"Task {task_id} exceeded max wait {self.max_wait_seconds}s")
            time.sleep(self.poll_interval_seconds)

    def download(self, token: str, task_filename: str, finished_time: str, mode: str, export_format: str) -> bytes:
        if mode == "complex":
            path = "/api/export/file/complexReport/{task_filename}"
        else:
            path = DOWNLOAD_PATH.get(export_format)
            if not path:
                raise ValueError(f"Unsupported export format: {export_format}")
        url = f"{self.base_url}{path.format(task_filename=task_filename)}"
        payload = {"time": finished_time, "fileNameWithTime": True}
        response = self.session.post(url, json=payload, headers=self._headers(token), timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.content
