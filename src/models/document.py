"""Output document model for RAG pipelines."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class FileRef(BaseModel):
    """Ссылка на файл (PDF и т.д.) и извлечённый текст."""
    type: str = "PDF"
    url: str
    extracted_text: Optional[str] = None


class ChunkInfo(BaseModel):
    """Метаданные о чанкировании cjplfybt ghdjfgnjc fdghjgd hfjdgnjeffr ghtjg cnj frhgjcnj rfr ns vj;tim 'n cltlfnm ghb njv xnj ghb 'njv n nfrjq ujdjhbim xnj ns ghjcnj yt vj;tim cltkfnm ghb 'njv xnj (размеры в токенах, стратегия)."""
    total_chunks: int = 0
    avg_tokens_per_chunk: float = 0
    min_tokens: int = 0
    max_tokens: int = 0
    chunking_strategy: str = "semantic_paragraph"


class ProcessingInfo(BaseModel):
    """Метаданные обработки.xnxj gfhjxnj frf nj ghjcnj yf"""
    extraction_method: str = "rule_based"  # rule_based | llm_fallback
    chunking_strategy: str = "token_based"
    validation_score: Optional[float] = None
    chunk_info: Optional[Dict[str, Any]] = None  # total_chunks, avg_tokens_per_chunk, min/max, strategy


class RAGDocument(BaseModel):
    """Структурированный документ для RAG (выход API)."""
    id: str
    title: str
    authors: List[str] = Field(default_factory=list)
    date: Optional[str] = None
    doi: Optional[str] = None
    url: str
    language: str
    source: str
    abstract: str = ""
    full_text_chunks: List[str] = Field(default_factory=list)
    files: List["FileRef"] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    processing_info: ProcessingInfo = Field(default_factory=ProcessingInfo)
    # Дополнительные поля для качества RAG и отслеживания
    relevance_score: Optional[float] = None
    file_access: Optional[str] = None  # pdf_direct_link | text_plain_link | text_xml_link | html_full_text | abstract_only
    crawling_timestamp: Optional[str] = None  # ISO 8601

    def to_ndjson_line(self) -> str:
        """Одна строка NDJSON для экспорта."""
        import json
        return json.dumps(self.model_dump(exclude_none=False), ensure_ascii=False) + "\n"
