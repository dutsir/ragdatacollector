"""Источник: OpenAlex (REST API, открытый). Метаданные и аннотации научных работ."""
from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlencode

import aiohttp

from ..models.document import FileRef, ProcessingInfo, RAGDocument
from ..processing import (
    chunk_text,
    compute_chunk_info,
    compute_validation_score,
    detect_language,
    document_type_from_text,
    filter_and_clean_chunks,
    is_dissertation,
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

OPENALEX_API_BASE = "https://api.openalex.org"
DEFAULT_USER_AGENT = "RAG-Collector/1.0 (https://github.com/rag-collector; mailto:support@example.org)"


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


def _abstract_from_inverted_index(inverted: Optional[dict]) -> str:
    """Восстанавливает текст аннотации из abstract_inverted_index OpenAlex."""
    if not inverted or not isinstance(inverted, dict):
        return ""
    position_to_word: dict[int, str] = {}
    for word, positions in inverted.items():
        for pos in positions:
            position_to_word[pos] = word
    if not position_to_word:
        return ""
    ordered = [position_to_word[i] for i in sorted(position_to_word.keys())]
    return " ".join(ordered)


def _normalize_lang_code(lang: str) -> str:
    """ISO 639-1 два символа."""
    if not lang or not isinstance(lang, str):
        return ""
    return lang.strip().lower().split("-")[0][:2]


def _work_to_source_result(work: dict, source_name: str = "OpenAlex") -> SourceResult:
    """Преобразует объект Work из OpenAlex API в SourceResult."""
    oa_id = (work.get("id") or "").strip()
    if oa_id.startswith("https://openalex.org/"):
        oa_id = oa_id.replace("https://openalex.org/", "", 1)
    doi = (work.get("doi") or "").strip()
    if doi and doi.startswith("https://doi.org/"):
        doi = doi.replace("https://doi.org/", "", 1)
    url = work.get("id") or (f"https://doi.org/{doi}" if doi else f"https://openalex.org/{oa_id}")
    if isinstance(url, str):
        url = url.strip()
    else:
        url = ""

    title = (work.get("display_name") or work.get("title") or "").strip()
    abstract = _abstract_from_inverted_index(work.get("abstract_inverted_index"))
    pub_date = (work.get("publication_date") or "").strip() or None
    lang_raw = (work.get("language") or "").strip()
    language = _normalize_lang_code(lang_raw) if lang_raw else None

    authors: list[str] = []
    for a in work.get("authorships") or []:
        author = a.get("author") if isinstance(a, dict) else None
        if isinstance(author, dict):
            name = (author.get("display_name") or "").strip()
            if name:
                authors.append(name)

    pdf_url: Optional[str] = None
    best_oa = work.get("best_oa_location")
    if isinstance(best_oa, dict):
        pdf_url = (best_oa.get("pdf_url") or best_oa.get("oa_url") or "").strip() or None
    for loc in work.get("locations") or []:
        if not pdf_url and isinstance(loc, dict):
            pdf_url = (loc.get("pdf_url") or loc.get("oa_url") or "").strip() or None
            if pdf_url:
                break

    meta: dict[str, Any] = {
        "openalex_id": oa_id,
        "language": language,
        "primary_location": work.get("primary_location"),
        "open_access": work.get("open_access"),
        "type": work.get("type"),
    }
    if work.get("primary_location") and isinstance(work["primary_location"], dict):
        src = work["primary_location"].get("source")
        if isinstance(src, dict):
            meta["container_title"] = src.get("display_name")
            meta["issn"] = src.get("issn_l")

    return SourceResult(
        title=title,
        url=url,
        authors=authors,
        date=pub_date,
        doi=doi or None,
        abstract=abstract,
        full_text="",
        pdf_url=pdf_url,
        metadata=meta,
        source_name=source_name,
    )


class OpenAlexSource(BaseSource):
    """Поиск и загрузка метаданных через OpenAlex REST API (без ключа, CC0)."""

    name = "openalex"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._base = OPENALEX_API_BASE
        self._user_agent = DEFAULT_USER_AGENT
        self._timeout = 30
        self._request_delay = 1.0
        try:
            import yaml
            root = Path(__file__).resolve().parent.parent.parent
            cfg_path = root / "config" / "settings.yaml"
            if cfg_path.exists():
                with open(cfg_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                cfg = (data.get("sources") or {}).get("openalex") or {}
                self._timeout = int(cfg.get("timeout", self._timeout))
                self._request_delay = float(cfg.get("request_delay", self._request_delay))
                if cfg.get("user_agent"):
                    self._user_agent = cfg["user_agent"]
        except Exception:
            pass

    def _headers(self) -> dict:
        return {"User-Agent": self._user_agent}

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    headers=self._headers(),
                    timeout=aiohttp.ClientTimeout(total=self._timeout),
                )
            return self._session

    async def close(self):
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None

    async def _request(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self._base}{path}"
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"
        session = await self._get_session()
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=self._timeout)) as resp:
            if resp.status == 429 or resp.status == 403:
                raise SourceTemporarilyUnavailableError(
                    f"OpenAlex API rate limit or blocked: {resp.status}",
                    source_name=self.name,
                )
            resp.raise_for_status()
            return await resp.json()

    async def search(
        self,
        query: str,
        *,
        max_results: int = 50,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        languages: Optional[list[str]] = None,
    ) -> list[SourceResult]:
        """Поиск работ по ключевым словам (search по title/abstract)."""
        if not (query or "").strip():
            print("[openalex] Empty query, returning empty results", flush=True)
            return []
        query_clean = query.strip()[:500]
        # Проверяем наличие кириллицы в запросе
        has_cyrillic = any('\u0400' <= char <= '\u04FF' for char in query_clean)
        if has_cyrillic:
            logger.warning("OpenAlex: query contains Cyrillic characters. OpenAlex API may not handle Cyrillic queries well, results may be irrelevant.")
            print(f"[openalex] WARNING: Query contains Cyrillic characters. OpenAlex may return irrelevant results.", flush=True)
        print(f"[openalex] Starting search: query='{query_clean[:100]}', max_results={max_results}, date_from={date_from}, date_to={date_to}", flush=True)
        results: list[SourceResult] = []
        per_page = min(25, max(max_results, 1))
        page = 1
        total_collected = 0

        while total_collected < max_results:
            params: dict[str, Any] = {
                "search": query_clean,
                "per-page": per_page,
                "page": page,
            }
            filters = []
            if date_from:
                # OpenAlex: YYYY-MM-DD
                d_from = (date_from or "").strip()[:10]
                if len(d_from) >= 10 and d_from[4] == "-" and d_from[7] == "-":
                    filters.append(f"from_publication_date:{d_from}")
            if date_to:
                d_to = (date_to or "").strip()[:10]
                if len(d_to) >= 10 and d_to[4] == "-" and d_to[7] == "-":
                    filters.append(f"to_publication_date:{d_to}")
            if filters:
                params["filter"] = ",".join(filters)

            print(f"[openalex] Requesting OpenAlex API with params: {params}", flush=True)
            try:
                data = await self._request("/works", params=params)
                print(f"[openalex] Received response from OpenAlex API", flush=True)
            except SourceTemporarilyUnavailableError as e:
                print(f"[openalex] SourceTemporarilyUnavailableError: {e}", flush=True)
                raise
            except Exception as e:
                logger.exception("OpenAlex search failed: %s", e)
                print(f"[openalex] Exception during search: {e}", flush=True)
                import traceback
                print(f"[openalex] Traceback: {traceback.format_exc()}", flush=True)
                break

            meta = data.get("meta") or {}
            items = data.get("results") or []
            total_results = meta.get("count", 0)
            print(f"[openalex] OpenAlex API returned {len(items)} items (page {page}), total_results={total_results}", flush=True)
            if not items:
                print(f"[openalex] No items in response, breaking", flush=True)
                break

            for work in items:
                if total_collected >= max_results:
                    break
                try:
                    result = _work_to_source_result(work, source_name=self.name)
                    result.metadata["_original_query"] = query_clean
                    results.append(result)
                    total_collected += 1
                except Exception as e:
                    logger.debug("OpenAlex skip item: %s", e)

            if len(items) < per_page:
                break
            page += 1
            await asyncio.sleep(self._request_delay)

        if languages:
            allowed = {_normalize_lang_code(l) for l in languages if l and isinstance(l, str)}
            if allowed:
                filtered = [r for r in results if (r.metadata or {}).get("language") in allowed]
                if len(filtered) != len(results):
                    logger.info(
                        "OpenAlex: language filter %s -> %s (allowed: %s)",
                        len(results), len(filtered), allowed,
                    )
                results = filtered[:max_results]
            else:
                results = results[:max_results]
        else:
            results = results[:max_results]

        logger.info("OpenAlex: query '%s' -> %s results", query_clean[:80], len(results))
        print(f"[openalex] Final results: {len(results)} documents", flush=True)
        return results

    async def fetch_article(self, url: str) -> Optional[SourceResult]:
        """Загрузка работы по OpenAlex URL (https://openalex.org/W...) или DOI (https://doi.org/...)."""
        url = url.strip()
        if "openalex.org/" in url:
            oa_id = url.split("openalex.org/")[-1].split("?")[0].rstrip("/")
            if not oa_id.startswith("W"):
                return None
            path = f"/works/{oa_id}"
        elif "doi.org/" in url:
            doi = url.split("doi.org/")[-1].split("?")[0].rstrip("/")
            path = f"/works/doi:{quote(doi, safe='')}"
        else:
            return None
        try:
            data = await self._request(path)
        except SourceTemporarilyUnavailableError:
            raise
        except Exception as e:
            logger.warning("OpenAlex fetch_article %s: %s", url, e)
            return None
        if not data:
            return None
        return _work_to_source_result(data, source_name=self.name)

    def to_rag_document(
        self,
        result: SourceResult,
        *,
        chunk_size_min: int = 500,
        chunk_size_max: int = 2000,
        overlap: int = 100,
    ) -> RAGDocument:
        """SourceResult -> RAGDocument с чанкованием по аннотации/тексту."""
        raw_id = f"{result.url}{result.title}"
        doc_id = hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:32]

        text_to_chunk = (result.abstract or "").strip()
        if text_to_chunk:
            _, text_to_chunk = validate_text_ends_complete(text_to_chunk)

        chunk_size_min = int(_text_processing_config("chunk_size_tokens_min", 500) or 500)
        chunk_size_max = int(_text_processing_config("chunk_size_tokens_max", 2000) or 2000)
        overlap = int(_text_processing_config("chunk_overlap_tokens", 100) or 100)
        max_chunks = int(_text_processing_config("max_chunks_per_document", 25) or 25)
        use_emb = _text_processing_config("use_embedding_chunker", False)
        sim_thr = _text_processing_config("similarity_threshold", 0.5)
        chunk_size_min = max(100, min(chunk_size_min, chunk_size_max))
        chunk_size_max = max(chunk_size_min, chunk_size_max)

        try:
            chunks = chunk_text(
                text_to_chunk,
                chunk_size_min=chunk_size_min,
                chunk_size_max=chunk_size_max,
                overlap_tokens=overlap,
                max_chunks=max_chunks,
                use_embedding_chunker=use_emb,
                similarity_threshold=float(sim_thr) if sim_thr else 0.5,
                embedding_model=_text_processing_config("embedding_model", "sentence-transformers/all-MiniLM-L6-v2") or "sentence-transformers/all-MiniLM-L6-v2",
            ) if text_to_chunk else []
        except MemoryError:
            fallback = (result.abstract or "")[:8000]
            chunks = [fallback] if fallback.strip() else []

        chunks = ensure_chunks_end_at_boundaries(chunks)
        chunks = dedupe_chunks_by_hash(chunks)
        chunks = filter_and_clean_chunks(chunks)
        chunk_strategy = "semantic_embedding" if use_emb else "semantic_paragraph"
        chunk_info = compute_chunk_info(chunks, strategy=chunk_strategy)

        language = (result.metadata or {}).get("language")
        if not language and (result.abstract or "").strip():
            try:
                language = _normalize_lang_code(detect_language((result.abstract or "")[:5000]))
            except Exception:
                pass
        if not language:
            language = "en"

        file_access = "abstract_only"
        if result.pdf_url:
            file_access = "pdf_direct_link"

        metadata = {k: v for k, v in (result.metadata or {}).items() if not k.startswith("_")}
        title_abstract = (result.title or "") + " " + (result.abstract or "")
        metadata["document_type"] = document_type_from_text(result.title or "", result.abstract or "", "")
        # OpenAlex передаёт type из API (article, dissertation и т.д.)
        api_type = (result.metadata or {}).get("type") or ""
        metadata["is_dissertation"] = (
            api_type == "dissertation"
            or (not api_type and is_dissertation(title_abstract))
        )

        files: list[FileRef] = []
        if result.pdf_url:
            files.append(FileRef(type="PDF", url=result.pdf_url))

        doc = RAGDocument(
            id=doc_id,
            title=result.title or "Без названия",
            authors=result.authors,
            date=normalize_date(result.date) if result.date else result.date,
            doi=result.doi,
            url=result.url,
            language=language,
            source="OpenAlex",
            abstract=result.abstract or "",
            full_text_chunks=chunks,
            files=files,
            metadata=metadata,
            processing_info=ProcessingInfo(
                extraction_method="api",
                chunking_strategy=chunk_strategy,
                validation_score=0.0,
                chunk_info=chunk_info,
            ),
            file_access=file_access,
            crawling_timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        doc.processing_info.validation_score = compute_validation_score(doc)
        return doc


register_source("openalex", OpenAlexSource)
