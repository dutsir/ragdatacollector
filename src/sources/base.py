"""Базовый класс источника данных."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from ..models.document import RAGDocument


class SourceTemporarilyUnavailableError(Exception):
    """Источник временно недоступен (капча, блокировка и т.д.). Сбор по другим источникам не прерывать."""
    def __init__(self, message: str, source_name: str = ""):
        self.source_name = source_name
        super().__init__(message)


@dataclass
class SourceResult:
    """Сырой результат от источника (до обработки в RAGDocument)."""
    title: str
    url: str
    authors: list[str] = field(default_factory=list)
    date: str | None = None
    doi: str | None = None
    abstract: str = ""
    full_text: str = ""
    pdf_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    source_name: str = ""


class BaseSource(ABC):
    """Базовый класс для всех источников."""

    name: str = "base"

    @abstractmethod
    async def search(
        self,
        query: str,
        *,
        max_results: int = 50,
        date_from: str | None = None,
        date_to: str | None = None,
        languages: list[str] | None = None,
    ) -> list[SourceResult]:
        """Поиск по запросу. Возвращает список сырых результатов."""
        ...

    @abstractmethod
    async def fetch_article(self, url: str) -> SourceResult | None:
        """Загрузка полного текста статьи по URL (опционально)."""
        ...
