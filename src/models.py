"""Pydantic models for API request/response."""

from pydantic import BaseModel


class PipelineStartRequest(BaseModel):
    since: str = "2024-01-01"
    headless: bool = True
    company_list: str = ""
    company_ids: list[str] | None = None  # magna_ids to scrape (subset of ta125)


class PipelineStatusResponse(BaseModel):
    status: str = "idle"  # idle|running|done|error
    progress: str = ""
    processed: int = 0
    total: int = 0
    error: str | None = None
    started_at: str | None = None


class PipelineLogResponse(BaseModel):
    lines: list[str] = []


class PaginatedResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    size: int


class StatsResponse(BaseModel):
    reports: dict
    attachments: dict
