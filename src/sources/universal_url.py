"""Универсальный парсер произвольных URL (Задача 3 из ТЗ).

Загружает HTML/PDF по URL, извлекает заголовок, текст, метаданные.
Используется как fallback в режиме target_site когда URL не принадлежит
конкретному зарегистрированному источнику.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup, Comment

from ..models.document import FileRef, ProcessingInfo, RAGDocument
from ..processing import (
    chunk_text,
    compute_chunk_info,
    compute_validation_score,
    detect_language,
    document_type_from_text,
    filter_and_clean_chunks,
)
from ..processing.pdf_extract import extract_text_from_pdf_url
from ..processing.validation import (
    dedupe_chunks_by_hash,
    ensure_chunks_end_at_boundaries,
    normalize_date,
    validate_text_ends_complete,
)
from .base import BaseSource, SourceResult, SourceTemporarilyUnavailableError
from .registry import register_source

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Теги, которые не несут основного текста
_STRIP_TAGS = {
    "script", "style", "nav", "header", "footer", "aside",
    "form", "button", "iframe", "noscript", "svg", "math",
    "figure", "figcaption", "menu", "menuitem",
}


def _text_processing_config(key: str, default):
    try:
        import yaml
        root = Path(__file__).resolve().parent.parent.parent
        cfg_path = root / "config" / "settings.yaml"
        if cfg_path.exists():
            with open(cfg_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return (data.get("text_processing") or {}).get(key, default)
        return default
    except Exception:
        return default


def _normalize_lang_code(lang: str) -> str:
    if not lang or not isinstance(lang, str):
        return ""
    return lang.strip().lower().split("-")[0][:2]


def _extract_text_from_html(html: str) -> tuple[str, str, list[str], Optional[str]]:
    """Извлекает (title, main_text, authors, date) из HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Удаляем ненужные теги
    for tag in soup.find_all(_STRIP_TAGS):
        tag.decompose()
    # Удаляем комментарии
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    # Заголовок
    title = ""
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.get_text(strip=True)
    # Приоритет: <h1>, <meta og:title>
    h1 = soup.find("h1")
    if h1:
        h1_text = h1.get_text(strip=True)
        if h1_text and len(h1_text) > 5:
            title = h1_text
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        title = og_title["content"]

    # Авторы из meta
    authors = []
    for meta in soup.find_all("meta", attrs={"name": re.compile(r"author", re.I)}):
        if meta.get("content"):
            authors.append(meta["content"])

    # Дата из meta
    date = None
    for meta_name in ["date", "article:published_time", "DC.date", "citation_date"]:
        meta = soup.find("meta", attrs={"name": meta_name}) or soup.find("meta", attrs={"property": meta_name})
        if meta and meta.get("content"):
            date = meta["content"]
            break

    # Основной текст: ищем <article>, <main>, <div role=main>, или <body>
    main_content = (
        soup.find("article")
        or soup.find("main")
        or soup.find("div", role="main")
        or soup.find("div", class_=re.compile(r"(content|article|post|entry)", re.I))
        or soup.find("body")
    )
    if main_content:
        # Собираем параграфы
        paragraphs = []
        for p in main_content.find_all(["p", "li", "h2", "h3", "h4", "blockquote", "pre"]):
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                paragraphs.append(text)
        main_text = "\n\n".join(paragraphs)
    else:
        main_text = soup.get_text(separator="\n", strip=True)

    # Очистка множественных пробелов/переносов
    main_text = re.sub(r"\n{3,}", "\n\n", main_text)
    main_text = re.sub(r"[ \t]{2,}", " ", main_text)

    return title, main_text.strip(), authors, date


class UniversalUrlSource(BaseSource):
    """Универсальный парсер произвольных URL."""

    name = "universal_url"

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._timeout = 30
        self._user_agent = DEFAULT_USER_AGENT
        self._request_delay = 2.0

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    headers={"User-Agent": self._user_agent},
                    timeout=aiohttp.ClientTimeout(total=self._timeout),
                )
            return self._session

    async def close(self):
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None

    async def search(
        self,
        query: str,
        *,
        max_results: int = 50,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        languages: Optional[list[str]] = None,
    ) -> list[SourceResult]:
        """Для universal_url search не используется — только fetch_article."""
        return []

    async def fetch_article(self, url: str) -> Optional[SourceResult]:
        """Загружает страницу по URL и извлекает текст, заголовок, метаданные.
        Поддерживает HTML-страницы и PDF-файлы.
        """
        url = url.strip()
        if not url:
            return None

        print(f"[universal_url] Fetching: {url[:120]}", flush=True)

        # Проверяем, PDF ли это по расширению
        parsed_url = urlparse(url)
        is_pdf = parsed_url.path.lower().endswith(".pdf")

        if is_pdf:
            return await self._fetch_pdf(url)

        session = await self._get_session()
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=self._timeout),
                allow_redirects=True,
            ) as resp:
                if resp.status != 200:
                    logger.warning("[universal_url] HTTP %s for %s", resp.status, url[:100])
                    return None

                content_type = (resp.headers.get("Content-Type") or "").lower()

                # PDF по Content-Type
                if "application/pdf" in content_type:
                    return await self._fetch_pdf(url)

                # Не HTML — пропускаем
                if "text/html" not in content_type and "text/plain" not in content_type:
                    logger.info("[universal_url] Unsupported content type '%s' for %s", content_type, url[:100])
                    return None

                html = await resp.text()

        except Exception as e:
            logger.warning("[universal_url] Failed to fetch %s: %s", url[:100], e)
            print(f"[universal_url] Error fetching {url[:100]}: {e}", flush=True)
            return None

        if not html or len(html) < 100:
            return None

        title, main_text, authors, date = _extract_text_from_html(html)

        if not main_text or len(main_text) < 50:
            logger.info("[universal_url] Too little text extracted from %s", url[:100])
            return None

        print(f"[universal_url] Extracted: title='{title[:60]}', text_len={len(main_text)}, authors={len(authors)}", flush=True)

        return SourceResult(
            title=title or "Без названия",
            url=url,
            authors=authors,
            date=date,
            abstract=main_text[:2000] if len(main_text) > 2000 else main_text,
            full_text=main_text,
            pdf_url=None,
            metadata={
                "document_type": "web_page",
                "content_length": len(main_text),
            },
            source_name=self.name,
        )

    async def _fetch_pdf(self, url: str) -> Optional[SourceResult]:
        """Извлекает текст из PDF по URL."""
        try:
            text = extract_text_from_pdf_url(url, timeout_sec=25, max_bytes=15 * 1024 * 1024)
        except Exception as e:
            logger.warning("[universal_url] PDF extraction failed for %s: %s", url[:100], e)
            return None

        if not text or len(text) < 100:
            return None

        # Пытаемся вытащить заголовок из первых строк PDF
        lines = text.strip().split("\n")
        title = ""
        for line in lines[:5]:
            line = line.strip()
            if len(line) > 10 and len(line) < 300:
                title = line
                break

        print(f"[universal_url] PDF extracted: title='{title[:60]}', text_len={len(text)}", flush=True)

        return SourceResult(
            title=title or "PDF document",
            url=url,
            authors=[],
            date=None,
            abstract=text[:2000],
            full_text=text,
            pdf_url=url,
            metadata={
                "document_type": "pdf",
                "content_length": len(text),
            },
            source_name=self.name,
        )

    def to_rag_document(
        self,
        result: SourceResult,
        *,
        chunk_size_min: int = 500,
        chunk_size_max: int = 2000,
        overlap: int = 100,
    ) -> RAGDocument:
        """SourceResult -> RAGDocument."""
        raw_id = f"{result.url}{result.title}"
        doc_id = hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:32]

        # Текст для чанкования: full_text > abstract
        text_to_chunk = (result.full_text or result.abstract or "").strip()
        if text_to_chunk:
            _, text_to_chunk = validate_text_ends_complete(text_to_chunk)

        chunk_size_min = int(_text_processing_config("chunk_size_tokens_min", 500) or 500)
        chunk_size_max = int(_text_processing_config("chunk_size_tokens_max", 2000) or 2000)
        overlap = int(_text_processing_config("chunk_overlap_tokens", 100) or 100)
        max_chunks = int(_text_processing_config("max_chunks_per_document", 25) or 25)
        chunk_size_min = max(100, min(chunk_size_min, chunk_size_max))
        chunk_size_max = max(chunk_size_min, chunk_size_max)

        try:
            chunks = chunk_text(
                text_to_chunk,
                chunk_size_min=chunk_size_min,
                chunk_size_max=chunk_size_max,
                overlap_tokens=overlap,
                max_chunks=max_chunks,
            ) if text_to_chunk else []
        except Exception:
            chunks = [text_to_chunk[:8000]] if text_to_chunk else []

        chunks = ensure_chunks_end_at_boundaries(chunks)
        chunks = dedupe_chunks_by_hash(chunks)
        chunks = filter_and_clean_chunks(chunks)
        chunk_strategy = "semantic_paragraph"
        chunk_info = compute_chunk_info(chunks, strategy=chunk_strategy)

        language = ""
        if result.full_text or result.abstract or result.title:
            try:
                sample = (result.abstract or result.full_text or result.title or "")[:5000]
                language = _normalize_lang_code(detect_language(sample))
            except Exception:
                pass
        if not language:
            language = "en"

        metadata = {k: v for k, v in (result.metadata or {}).items() if not k.startswith("_")}

        # Файлы
        files = []
        if result.pdf_url:
            files.append(FileRef(
                url=result.pdf_url,
                file_type="pdf",
                extracted_text=(result.full_text or "")[:50000],
            ))

        file_access = "html_full_text"
        if result.pdf_url:
            file_access = "pdf_direct_link"
        elif chunks:
            file_access = "html_full_text"
        else:
            file_access = "metadata_only"

        doc = RAGDocument(
            id=doc_id,
            title=result.title or "Без названия",
            authors=result.authors or [],
            date=normalize_date(result.date) if result.date else None,
            doi=result.doi,
            url=result.url,
            language=language,
            source="Universal URL Parser",
            abstract=(result.abstract or "")[:5000],
            full_text_chunks=chunks,
            files=files,
            metadata=metadata,
            processing_info=ProcessingInfo(
                extraction_method="html_parser" if not result.pdf_url else "pdf_extract",
                chunking_strategy=chunk_strategy,
                validation_score=0.0,
                chunk_info=chunk_info,
            ),
            file_access=file_access,
            crawling_timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        doc.processing_info.validation_score = compute_validation_score(doc)
        return doc


register_source("universal_url", UniversalUrlSource)
