from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, Field


class JobCreate(BaseModel):
    task_type: Literal["echo", "sleep", "fail"]
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=0, le=100)
    max_retries: int = Field(default=3, ge=0, le=10)
    scheduled_at: datetime | None = None


class JobOut(BaseModel):
    id: str
    task_type: str
    payload: dict[str, Any]
    priority: int
    status: str
    attempts: int
    max_retries: int
    result: str | None = None
    error: str | None = None
    created_at: str
    scheduled_at: str
    started_at: str | None = None
    completed_at: str | None = None
