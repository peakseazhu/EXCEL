from __future__ import annotations

from typing import List, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator


class ProjectConfig(BaseModel):
    name: str = "bi-pipeline"
    timezone: str = "Asia/Shanghai"
    data_dir: str = "data"
    log_dir: str = "logs"
    export_format: Literal["csv", "xlsx"] = "csv"
    request_timeout_seconds: int = 30
    request_max_retries: int = 5
    task_poll_interval_seconds: int = 5
    task_max_wait_seconds: int = 1800


class FilterRules(BaseModel):
    update_date: bool = True
    days_ago_start: int = 1
    days_ago_end: int = 1
    update_month: bool = True
    month: Optional[str] = None


class ChartConfig(BaseModel):
    chart_id: str
    name: str
    export_format: Optional[Literal["csv", "xlsx"]] = None
    filters: List[dict] = Field(default_factory=list)
    filter_rules: FilterRules = Field(default_factory=FilterRules)
    schema_path: Optional[str] = None


class BIConfig(BaseModel):
    base_url: str
    domain: str = "guanbi"
    username_env: str = "BI_USERNAME"
    password_env: str = "BI_PASSWORD"
    charts: List[ChartConfig]

    @field_validator("charts")
    @classmethod
    def ensure_unique_chart_ids(cls, value: List[ChartConfig]) -> List[ChartConfig]:
        ids = [c.chart_id for c in value]
        if len(ids) != len(set(ids)):
            raise ValueError("chart_id must be unique")
        return value


class TargetTableConfig(BaseModel):
    name: str
    source: Literal["file", "feishu"] = "file"
    path: Optional[str] = None
    schema_path: Optional[str] = None
    feishu_sheet_name: Optional[str] = None


class TargetsConfig(BaseModel):
    tables: List[TargetTableConfig]

    @field_validator("tables")
    @classmethod
    def ensure_unique_target_names(cls, value: List[TargetTableConfig]) -> List[TargetTableConfig]:
        names = [t.name for t in value]
        if len(names) != len(set(names)):
            raise ValueError("target table name must be unique")
        return value


class OutputSheetConfig(BaseModel):
    sheet_name: str
    table: str
    start_cell: str = "A1"
    batch_size: int = 5000
    clear_extra_rows: bool = True


class AlertConfig(BaseModel):
    receive_id_type: Literal["user_id", "chat_id"] = "user_id"
    receive_id: str


class FeishuConfig(BaseModel):
    app_id_env: str = "FEISHU_APP_ID"
    app_secret_env: str = "FEISHU_APP_SECRET"
    spreadsheet_token: str
    outputs: List[OutputSheetConfig]
    alert: Optional[AlertConfig] = None

    @field_validator("outputs")
    @classmethod
    def ensure_unique_sheets(cls, value: List[OutputSheetConfig]) -> List[OutputSheetConfig]:
        names = [o.sheet_name for o in value]
        if len(names) != len(set(names)):
            raise ValueError("sheet_name must be unique")
        return value


class PipelineConfig(BaseModel):
    project: ProjectConfig
    bi: BIConfig
    targets: TargetsConfig
    feishu: FeishuConfig

    @model_validator(mode="after")
    def validate_exports(self) -> "PipelineConfig":
        for chart in self.bi.charts:
            if chart.export_format and chart.export_format not in {"csv", "xlsx"}:
                raise ValueError("export_format must be csv or xlsx")
        return self
