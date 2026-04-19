"""Источник: DuckDuckGo SERP (HTML-парсинг, без API-ключа).
Поиск веб-страниц по ключевым словам с извлечением snippet, URL, title.
Соблюдает задержки между запросами. Без обхода ToS.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlencode, urlparse, parse_qs, unquote

import aiohttp
from bs4 import BeautifulSoup

from ..models.document import FileRef, ProcessingInfo, RAGDocument
from ..processing import (
    chunk_text,
    compute_chunk_info,
    compute_validation_score,
    detect_language,
    document_type_from_text,
    filter_and_clean_chunks,
)
from ..processing.validation import (
    dedupe_chunks_by_hash,
    ensure_chunks_end_at_boundaries,
    normalize_date,
    validate_text_ends_complete,
)
from .base import BaseSource, SourceResult, SourceTemporarilyUnavailableError
from .registry import register_source

logger = logging.getLogger(__name__)

# DuckDuckGo HTML search — публичный, без API-ключа
DUCKDUCKGO_HTML_URL = "https://html.duckduckgo.com/html/"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _text_processing_config(key: str, default):
    """Читает config/settings.yaml -> text_processing.<key>."""
    try:
        import yaml
        root = Path(__file__).resolve().parent.parent.parent
        cfg_path = root / "config" / "settings.yaml"
        if cfg_path.exists():
            with open(cfg_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            val = (data.get("text_processing") or {}).get(key, default)
            return val
        return default
    except Exception:
        return default


def _normalize_lang_code(lang: str) -> str:
    if not lang or not isinstance(lang, str):
        return ""
    return lang.strip().lower().split("-")[0][:2]


def _extract_real_url(raw_url: str) -> str:
    """Извлекает настоящий URL из DuckDuckGo redirect-ссылки.
    Пример: //duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com&... -> https://example.com
    """
    if not raw_url:
        return raw_url
    # DuckDuckGo оборачивает ссылки в redirect
    if "duckduckgo.com/l/" in raw_url or "uddg=" in raw_url:
        parsed = urlparse(raw_url)
        qs = parse_qs(parsed.query)
        real = qs.get("uddg", [None])[0]
        if real:
            return unquote(real)
    # Если URL начинается с //, добавляем https:
    if raw_url.startswith("//"):
        return "https:" + raw_url
    return raw_url


def _parse_result(result_div) -> Optional[dict]:
    """Парсит один результат из DuckDuckGo HTML."""
    try:
        # Заголовок и URL — пробуем разные CSS-классы DuckDuckGo
        link_el = (
            result_div.find("a", class_="result__a")
            or result_div.find("a", class_="result-link")
            or result_div.find("h2", class_="result__title")  # внутри h2 может быть <a>
        )
        if link_el and link_el.name == "h2":
            link_el = link_el.find("a")
        if not link_el:
            # Последний fallback — первая ссылка в div
            link_el = result_div.find("a", href=True)
        if not link_el:
            return None
        
        title = link_el.get_text(strip=True)
        raw_url = link_el.get("href", "")
        if not raw_url or not title:
            return None
        url = _extract_real_url(raw_url)

        # Snippet — пробуем разные классы
        snippet_el = (
            result_div.find("a", class_="result__snippet")
            or result_div.find("div", class_="result__snippet")
            or result_div.find("span", class_="result__snippet")
            or result_div.find("td", class_="result-snippet")
        )
        snippet = snippet_el.get_text(strip=True) if snippet_el else ""

        return {
            "title": title,
            "url": url,
            "snippet": snippet,
        }
    except Exception:
        return None


class DuckDuckGoSerpSource(BaseSource):
    """Поиск веб-страниц через DuckDuckGo HTML (без API-ключа)."""

    name = "google_serp"  # Имя в API совпадает с ТЗ

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._timeout = 30
        self._request_delay = 3.0
        self._user_agent = DEFAULT_USER_AGENT
        try:
            import yaml
            root = Path(__file__).resolve().parent.parent.parent
            cfg_path = root / "config" / "settings.yaml"
            if cfg_path.exists():
                with open(cfg_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                cfg = (data.get("sources") or {}).get("google_serp") or {}
                self._timeout = int(cfg.get("timeout", self._timeout))
                self._request_delay = float(cfg.get("request_delay", self._request_delay))
                if cfg.get("user_agent"):
                    self._user_agent = cfg["user_agent"]
        except Exception:
            pass

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
        """Поиск по DuckDuckGo HTML. Возвращает до max_results результатов."""
        if not (query or "").strip():
            return []

        query_clean = query.strip()[:500]
        print(f"[google_serp] Starting DuckDuckGo HTML search: query='{query_clean[:100]}', max_results={max_results}", flush=True)

        # Формируем параметры
        # DuckDuckGo HTML принимает POST с полем q
        # Локализация через kl: ru-ru, en-us, fr-fr и т.д.
        kl = ""
        if languages:
            lang_to_kl = {
                "ru": "ru-ru", "en": "us-en", "fr": "fr-fr", "de": "de-de",
                "es": "es-es", "zh": "cn-zh", "ja": "jp-jp", "ko": "kr-kr",
                "ar": "xa-ar", "he": "il-he",
            }
            for lang in languages:
                if lang in lang_to_kl:
                    kl = lang_to_kl[lang]
                    break

        results: list[SourceResult] = []
        session = await self._get_session()

        try:
            form_data = {"q": query_clean, "b": ""}
            if kl:
                form_data["kl"] = kl

            async with session.post(
                DUCKDUCKGO_HTML_URL,
                data=form_data,
                timeout=aiohttp.ClientTimeout(total=self._timeout),
            ) as resp:
                if resp.status == 403 or resp.status == 429:
                    raise SourceTemporarilyUnavailableError(
                        f"DuckDuckGo rate limit or blocked: {resp.status}",
                        source_name=self.name,
                    )
                if resp.status != 200:
                    logger.warning("DuckDuckGo SERP: status %s", resp.status)
                    return []

                html = await resp.text()

            soup = BeautifulSoup(html, "html.parser")
            
            # DuckDuckGo HTML использует разные CSS-классы в разных версиях
            result_divs = soup.find_all("div", class_="result")
            if not result_divs:
                result_divs = soup.find_all("div", class_="results_links")
            if not result_divs:
                result_divs = soup.find_all("div", class_="result__body")
            if not result_divs:
                # Fallback: ищем все ссылки с заголовком
                result_divs = soup.find_all("div", class_="links_main")

            print(f"[google_serp] Found {len(result_divs)} result divs on page", flush=True)
            
            # Если нет результатов — сохраняем HTML для отладки
            if len(result_divs) <= 1:
                try:
                    from pathlib import Path
                    debug_dir = Path("debug")
                    debug_dir.mkdir(exist_ok=True)
                    (debug_dir / "duckduckgo_debug.html").write_text(html[:50000], encoding="utf-8")
                    print(f"[google_serp] Saved debug HTML to debug/duckduckgo_debug.html", flush=True)
                except Exception:
                    pass

            for div in result_divs:
                if len(results) >= max_results:
                    break

                parsed = _parse_result(div)
                if not parsed:
                    continue

                url = parsed["url"]
                title = parsed["title"]
                snippet = parsed["snippet"]

                # Пропускаем duckduckgo.com внутренние ссылки
                if "duckduckgo.com" in url:
                    continue

                sr = SourceResult(
                    title=title,
                    url=url,
                    abstract=snippet,
                    full_text="",
                    metadata={
                        "snippet": snippet,
                        "_original_query": query_clean,
                    },
                    source_name=self.name,
                )
                results.append(sr)

            await asyncio.sleep(self._request_delay)

        except SourceTemporarilyUnavailableError:
            raise
        except Exception as e:
            logger.exception("DuckDuckGo SERP search failed: %s", e)
            print(f"[google_serp] Exception: {e}", flush=True)

        print(f"[google_serp] Final results: {len(results)} documents", flush=True)
        return results[:max_results]

    async def fetch_article(self, url: str) -> Optional[SourceResult]:
        """Загрузка статьи по URL — для SERP не реализована детально."""
        return None

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

        text_to_chunk = (result.abstract or "").strip()
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
            chunks = [text_to_chunk] if text_to_chunk else []

        chunks = ensure_chunks_end_at_boundaries(chunks)
        chunks = dedupe_chunks_by_hash(chunks)
        chunks = filter_and_clean_chunks(chunks)
        chunk_strategy = "semantic_paragraph"
        chunk_info = compute_chunk_info(chunks, strategy=chunk_strategy)

        language = ""
        if result.abstract or result.title:
            try:
                language = _normalize_lang_code(
                    detect_language((result.abstract or result.title or "")[:5000])
                )
            except Exception:
                pass
        if not language:
            language = "en"

        metadata = {k: v for k, v in (result.metadata or {}).items() if not k.startswith("_")}
        metadata["document_type"] = "web_page"

        doc = RAGDocument(
            id=doc_id,
            title=result.title or "Без названия",
            authors=[],
            date=normalize_date(result.date) if result.date else None,
            doi=None,
            url=result.url,
            language=language,
            source="DuckDuckGo SERP",
            abstract=result.abstract or "",
            full_text_chunks=chunks,
            files=[],
            metadata=metadata,
            processing_info=ProcessingInfo(
                extraction_method="serp",
                chunking_strategy=chunk_strategy,
                validation_score=0.0,
                chunk_info=chunk_info,
            ),
            file_access="snippet_only",
            crawling_timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        doc.processing_info.validation_score = compute_validation_score(doc)
        return doc


register_source("google_serp", DuckDuckGoSerpSource)
