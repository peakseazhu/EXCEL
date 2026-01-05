from __future__ import annotations

from typing import List, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator


EXPORT_FORMATS = {"csv", "xlsx", "pivot"}
EXPORT_FALLBACKS = EXPORT_FORMATS | {"complex"}


def normalize_export_value(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("export format must be a string")
    normalized = value.strip().lower()
    if normalized == "excel":
        return "xlsx"
    if normalized in {"table", "pivot"}:
        return "pivot"
    return normalized


class ProjectConfig(BaseModel):
    name: str = "bi-pipeline"
    timezone: str = "Asia/Shanghai"
    data_dir: str = "data"
    log_dir: str = "logs"
    export_format: Literal["csv", "xlsx", "pivot"] = "csv"
    request_timeout_seconds: int = 30
    request_max_retries: int = 5
    task_poll_interval_seconds: int = 5
    task_max_wait_seconds: int = 1800
    profile_sample_rows: int = 100000

    @field_validator("export_format", mode="before")
    @classmethod
    def normalize_project_export_format(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return normalize_export_value(value)

    @field_validator("export_format")
    @classmethod
    def validate_project_export_format(cls, value: str) -> str:
        if value not in EXPORT_FORMATS:
            raise ValueError(f"export_format must be one of {sorted(EXPORT_FORMATS)}")
        return value


class CompareConfig(BaseModel):
    baseline_dir: str = "baseline"
    rounding: int = 2
    max_samples: int = 20


class FilterRules(BaseModel):
    update_date: bool = True
    days_ago_start: int = 1
    days_ago_end: int = 1
    update_month: bool = True
    month: Optional[str] = None


class ChartConfig(BaseModel):
    chart_id: str
    name: str
    export_format: Optional[Literal["csv", "xlsx", "pivot"]] = None
    export_mode: Literal["simple", "complex"] = "simple"
    export_fallbacks: Optional[List[str]] = None
    sheet_name: Optional[str] = None
    filters: List[dict] = Field(default_factory=list)
    filter_rules: FilterRules = Field(default_factory=FilterRules)
    schema_path: Optional[str] = None

    @field_validator("export_format", mode="before")
    @classmethod
    def normalize_chart_export_format(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return normalize_export_value(value)

    @field_validator("export_format")
    @classmethod
    def validate_chart_export_format(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        if value not in EXPORT_FORMATS:
            raise ValueError(f"export_format must be one of {sorted(EXPORT_FORMATS)}")
        return value

    @field_validator("export_fallbacks", mode="before")
    @classmethod
    def normalize_export_fallbacks(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return value
        raw = [value] if isinstance(value, str) else list(value)
        return [normalize_export_value(item) for item in raw]

    @field_validator("export_fallbacks")
    @classmethod
    def validate_export_fallbacks(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return value
        invalid = [item for item in value if item not in EXPORT_FALLBACKS]
        if invalid:
            raise ValueError(f"export_fallbacks contains unsupported values: {invalid}")
        return value


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
    include_header: bool = True


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
    compare: CompareConfig = Field(default_factory=CompareConfig)

    @model_validator(mode="after")
    def validate_exports(self) -> "PipelineConfig":
        for chart in self.bi.charts:
            if chart.export_format and chart.export_format not in EXPORT_FORMATS:
                raise ValueError(f"export_format must be one of {sorted(EXPORT_FORMATS)}")
        return self
