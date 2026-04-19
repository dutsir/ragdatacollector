"""API request/response and task models."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class TaskType(str, Enum):
    science = "science"
    web_search = "web_search"
    target_site = "target_site"


class TaskStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


def _normalize_sources_list(value: List[str]) -> List[str]:
    """
    Нормализует список источников: элементы вида "crossref, cyberleninka" разбиваются
    по запятой, пробелы убираются, дубликаты удаляются. Один запрос (keywords, dates,
    languages) применяется ко всем источникам; max_results задаёт лимит на каждый источник.
    """
    if not value:
        return ["cyberleninka"]
    out: List[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        for part in item.split(","):
            name = part.strip()
            if name and name.lower() not in seen:
                seen.add(name.lower())
                out.append(name)
    return out if out else ["cyberleninka"]


class CollectRequest(BaseModel):
    """POST /api/v1/collect body."""
    task_type: TaskType = TaskType.science
    keywords: List[str] = Field(..., min_length=1, description="Ключевые слова для поиска")
    date_from: Optional[str] = Field(None, description="Дата начала (YYYY-MM-DD)")
    date_to: Optional[str] = Field(None, description="Дата окончания (YYYY-MM-DD)")
    languages: List[str] = Field(
        default_factory=lambda: ["ru", "en"],
        description="Коды языков: ru, en, fr, de, es, zh, ja, ko, ar, he, yi",
    )
    sources: List[str] = Field(
        default_factory=lambda: ["cyberleninka"],
        description="Список источников (одинаковые keywords/dates/languages для всех). Пример: [\"crossref\", \"cyberleninka\", \"openalex\"]",
    )
    max_results: int = Field(
        50, ge=1, le=500,
        description="Максимум результатов с каждого источника (итого может быть до len(sources)*max_results)",
    )
    urls: Optional[List[str]] = Field(
        None,
        description="Конкретные URL для task_type=target_site",
    )

    @field_validator("keywords", mode="before")
    @classmethod
    def normalize_keywords(cls, v: Any) -> List[str]:
        """
        Очищает keywords от запятых и пробелов в конце.
        Если keyword содержит запятую внутри, разбивает на несколько keywords.
        """
        if isinstance(v, list):
            cleaned = []
            for kw in v:
                if isinstance(kw, str):
                    # Разбиваем по запятым, если они есть внутри
                    parts = [p.strip() for p in kw.split(",")]
                    for part in parts:
                        part = part.strip().rstrip(",").strip()
                        if part:
                            cleaned.append(part)
            return cleaned if cleaned else ["cyberleninka"]
        if isinstance(v, str):
            # Разбиваем по запятым, если они есть
            parts = [p.strip() for p in v.split(",")]
            cleaned = [p.strip().rstrip(",").strip() for p in parts if p.strip()]
            return cleaned if cleaned else ["cyberleninka"]
        return ["cyberleninka"]

    @field_validator("sources", mode="before")
    @classmethod
    def normalize_sources(cls, v: Any) -> List[str]:
        if isinstance(v, list):
            return _normalize_sources_list(v)
        if isinstance(v, str):
            return _normalize_sources_list([v])
        return _normalize_sources_list([])

    def date_range(self) -> Optional[Dict[str, str]]:
        if self.date_from and self.date_to:
            return {"from": self.date_from, "to": self.date_to}
        return None


class TaskInfo(BaseModel):
    """Информация о задаче в очереди."""
    task_id: str
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    request: CollectRequest
    result_count: int = 0
    error: Optional[str] = None


class CollectResponse(BaseModel):
    """Ответ на создание задачи сбора."""
    task_id: str
    status: TaskStatus = TaskStatus.pending
    message: str = "Task created"


class TaskStatusResponse(BaseModel):
    """Ответ со статусом задачи."""
    task_id: str
    status: TaskStatus
    result_count: int = 0
    error: Optional[str] = None
    documents: Optional[List[Dict[str, Any]]] = None  # при status=completed
