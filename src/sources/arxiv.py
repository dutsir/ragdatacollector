"""Источник: arXiv (REST API). Препринты по физике, математике, CS и др."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

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

ARXIV_API_BASE = "https://export.arxiv.org/api/query"
# Рекомендация arXiv: задержка ~3 сек между повторными запросами
DEFAULT_USER_AGENT = "RAG-Collector/1.0 (https://github.com/rag-collector; mailto:support@example.org)"

# Atom namespace
ATOM_NS = "http://www.w3.org/2005/Atom"
ARXIV_NS = "http://arxiv.org/schemas/atom"
OPENSEARCH_NS = "http://a9.com/-/spec/opensearch/1.1/"


def _text_processing_config(key: str, default):
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


def _elem_text(elem, default: str = "") -> str:
    if elem is None:
        return default
    return (elem.text or "").strip() or "".join(elem.itertext()).strip() or default


def _find_elem(parent, local_name: str, namespaces=None):
    """Ищет дочерний элемент по локальному имени, игнорируя namespace.
    Это надёжнее чем find() с конкретным namespace, потому что arXiv API
    может менять default namespace.
    """
    if parent is None:
        return None
    # Попытка 1: с известными namespace
    if namespaces:
        for ns in namespaces:
            el = parent.find(f"{{{ns}}}{local_name}")
            if el is not None:
                return el
    # Попытка 2: без namespace
    el = parent.find(local_name)
    if el is not None:
        return el
    # Попытка 3: перебор дочерних элементов по локальному имени
    for child in parent:
        tag = child.tag
        if isinstance(tag, str):
            # Убираем namespace: {http://...}name -> name
            local = tag.split("}")[-1] if "}" in tag else tag
            if local == local_name:
                return child
    return None


def _find_all_elems(parent, local_name: str, namespaces=None):
    """Ищет все дочерние элементы по локальному имени, игнорируя namespace."""
    if parent is None:
        return []
    results = []
    # Попытка с namespace
    if namespaces:
        for ns in namespaces:
            found = parent.findall(f".//{{{ns}}}{local_name}")
            if found:
                return found
    # Без namespace
    found = parent.findall(local_name)
    if found:
        return found
    # Перебор дочерних
    for child in parent:
        tag = child.tag
        if isinstance(tag, str):
            local = tag.split("}")[-1] if "}" in tag else tag
            if local == local_name:
                results.append(child)
    return results


def _parse_published_or_updated(published_elem, updated_elem) -> Optional[str]:
    """Из published/updated (ISO 8601) берём дату в YYYY-MM-DD."""
    for elem in (published_elem, updated_elem):
        if elem is None:
            continue
        text = _elem_text(elem)
        if not text:
            continue
        # 2007-02-27T16:02:02-05:00 -> 2007-02-27
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


_ATOM_NAMESPACES = [ATOM_NS]
_ARXIV_NAMESPACES = [ARXIV_NS]


def _entry_to_source_result(entry, source_name: str = "arXiv") -> Optional[SourceResult]:
    """Парсит один Atom <entry> в SourceResult. Пропускает error-записи."""
    if entry is None:
        return None
    # Error entry: id содержит api/errors
    id_el = _find_elem(entry, "id", _ATOM_NAMESPACES)
    entry_id = _elem_text(id_el)
    if "api/errors" in (entry_id or ""):
        return None

    title = _elem_text(_find_elem(entry, "title", _ATOM_NAMESPACES))
    summary = _elem_text(_find_elem(entry, "summary", _ATOM_NAMESPACES))
    published = _find_elem(entry, "published", _ATOM_NAMESPACES)
    updated = _find_elem(entry, "updated", _ATOM_NAMESPACES)
    pub_date = _parse_published_or_updated(published, updated)

    authors = []
    for author in _find_all_elems(entry, "author", _ATOM_NAMESPACES):
        name_el = _find_elem(author, "name", _ATOM_NAMESPACES)
        if name_el is not None:
            name = _elem_text(name_el)
            if name and name != "arXiv api core":
                authors.append(name)

    url = (entry_id or "").strip()
    pdf_url = None
    doi = None
    for link in _find_all_elems(entry, "link", _ATOM_NAMESPACES):
        rel = (link.get("rel") or "").strip()
        title_attr = (link.get("title") or "").strip().lower()
        href = (link.get("href") or "").strip()
        if rel == "related" and title_attr == "pdf" and href:
            pdf_url = href
        if rel == "related" and title_attr == "doi" and href:
            if "doi.org/" in href:
                doi = href.split("doi.org/")[-1].split("?")[0].rstrip("/")
            else:
                doi = href
    if not doi:
        doi_el = _find_elem(entry, "doi", _ARXIV_NAMESPACES)
        if doi_el is not None:
            doi = _elem_text(doi_el)

    meta: dict[str, Any] = {"url": url}
    primary_cat = _find_elem(entry, "primary_category", _ARXIV_NAMESPACES)
    if primary_cat is not None and primary_cat.get("term"):
        meta["primary_category"] = primary_cat.get("term")

    return SourceResult(
        title=title or "",
        url=url,
        authors=authors,
        date=pub_date,
        doi=doi or None,
        abstract=summary,
        full_text="",
        pdf_url=pdf_url,
        metadata=meta,
        source_name=source_name,
    )


class ArxivSource(BaseSource):
    """Поиск и загрузка препринтов через arXiv API (Atom)."""

    name = "arxiv"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._timeout = 60
        self._request_delay = 3.0  # arXiv рекомендует ~3 сек между запросами
        self._base = ARXIV_API_BASE
        try:
            import yaml
            root = Path(__file__).resolve().parent.parent.parent
            cfg_path = root / "config" / "settings.yaml"
            if cfg_path.exists():
                with open(cfg_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                cfg = (data.get("sources") or {}).get("arxiv") or {}
                self._timeout = int(cfg.get("timeout", self._timeout))
                self._request_delay = float(cfg.get("request_delay", self._request_delay))
                if cfg.get("base_url"):
                    self._base = cfg["base_url"].rstrip("/")
        except Exception:
            pass

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    headers={"User-Agent": DEFAULT_USER_AGENT},
                    timeout=aiohttp.ClientTimeout(total=self._timeout),
                )
            return self._session

    async def close(self):
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None

    async def _get(self, url: str) -> str:
        session = await self._get_session()
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=self._timeout)) as resp:
            if resp.status == 429 or resp.status == 503:
                raise SourceTemporarilyUnavailableError(
                    f"arXiv API rate limit or unavailable: {resp.status}",
                    source_name=self.name,
                )
            resp.raise_for_status()
            return await resp.text()

    def _build_search_query(self, query: str, date_from: Optional[str], date_to: Optional[str]) -> str:
        """Формирует search_query: all:term1+AND+all:term2.
        
        Примечание: submittedDate фильтр arXiv API ненадёжен и часто возвращает 0 результатов.
        Вместо этого фильтрация по дате выполняется post-hoc на стороне клиента.
        """
        terms = [t.strip() for t in query.split() if t.strip()]
        if not terms:
            return query.strip()
        return "+AND+".join(f"all:{t}" for t in terms)

    async def search(
        self,
        query: str,
        *,
        max_results: int = 50,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        languages: Optional[list[str]] = None,
    ) -> list[SourceResult]:
        """Поиск в arXiv по ключевым словам (all:...) с опциональным фильтром по дате подачи."""
        if not (query or "").strip():
            return []
        query_clean = query.strip()[:500]
        # Проверяем наличие кириллицы в запросе
        has_cyrillic = any('\u0400' <= char <= '\u04FF' for char in query_clean)
        if has_cyrillic:
            logger.warning("arXiv: query contains Cyrillic characters. arXiv API does not support Cyrillic, will return 0 results.")
            print(f"[arxiv] WARNING: Query contains Cyrillic characters. arXiv does not support Cyrillic, will return 0 results.", flush=True)
        search_query = self._build_search_query(query_clean, date_from, date_to)
        logger.info("arXiv: built search_query='%s' from query='%s', date_from=%s, date_to=%s", 
                    search_query[:200], query_clean[:100], date_from, date_to)
        results: list[SourceResult] = []
        start = 0
        per_request = min(100, max(max_results, 1))

        while len(results) < max_results:
            # Строим URL вручную: arXiv API требует незакодированные +, :, [] в search_query
            url = (
                f"{self._base}"
                f"?search_query={search_query}"
                f"&start={start}"
                f"&max_results={per_request}"
                f"&sortBy=relevance"
                f"&sortOrder=descending"
            )
            print(f"[arxiv] Requesting: {url[:200]}", flush=True)
            try:
                xml_text = await self._get(url)
                await asyncio.sleep(self._request_delay)
            except SourceTemporarilyUnavailableError:
                raise
            except Exception as e:
                logger.exception("arXiv search failed: %s", e)
                break

            try:
                root = ET.fromstring(xml_text)
            except ET.ParseError as e:
                logger.warning("arXiv XML parse error: %s", e)
                break

            entries = _find_all_elems(root, "entry", _ATOM_NAMESPACES)
            print(f"[arxiv] Found {len(entries)} entries in XML response", flush=True)
            if entries:
                # Дебаг: показываем первую запись
                first = entries[0]
                first_title = _elem_text(_find_elem(first, "title", _ATOM_NAMESPACES))
                first_id = _elem_text(_find_elem(first, "id", _ATOM_NAMESPACES))
                print(f"[arxiv] First entry: id='{first_id[:60]}', title='{first_title[:60]}'", flush=True)
            if not entries:
                # Проверяем, есть ли сообщение об ошибке
                total_el = _find_elem(root, "totalResults", [OPENSEARCH_NS])
                if total_el is not None:
                    print(f"[arxiv] opensearch:totalResults = {total_el.text}", flush=True)
                break

            for entry in entries:
                if len(results) >= max_results:
                    break
                res = _entry_to_source_result(entry, source_name=self.name)
                if res:
                    res.metadata["_original_query"] = query_clean
                    results.append(res)

            if len(entries) < per_request:
                break
            start += per_request

        # Post-hoc фильтрация по дате (вместо ненадёжного submittedDate в запросе)
        if date_from or date_to:
            before_date_filter = len(results)
            date_filtered = []
            for r in results:
                if not r.date:
                    date_filtered.append(r)  # Нет даты — не фильтруем
                    continue
                # Дата в формате YYYY-MM-DD или YYYY
                doc_date = r.date.strip()[:10]
                if date_from and doc_date < date_from:
                    continue
                if date_to and doc_date > date_to:
                    continue
                date_filtered.append(r)
            if len(date_filtered) != before_date_filter:
                logger.info("arXiv: date filter %s -> %s (from=%s, to=%s)", 
                           before_date_filter, len(date_filtered), date_from, date_to)
            results = date_filtered

        if languages:
            allowed = {_normalize_lang_code(l) for l in languages if l and isinstance(l, str)}
            if allowed:
                filtered = []
                for r in results:
                    lang = (r.metadata or {}).get("language")
                    if lang and lang in allowed:
                        filtered.append(r)
                    elif not lang:
                        # Язык не определён в метаданных — пробуем определить
                        sample = (r.abstract or r.title or "")[:3000]
                        if sample and len(sample.strip()) > 10:
                            try:
                                detected = _normalize_lang_code(detect_language(sample))
                                r.metadata["language"] = detected
                                if detected in allowed:
                                    filtered.append(r)
                                else:
                                    # Язык определён, но не в списке — всё равно пропускаем (arXiv публикации на en)
                                    print(f"[arxiv] language filter: '{(r.title or '')[:50]}' detected as '{detected}', not in {allowed}, keeping anyway", flush=True)
                                    filtered.append(r)
                            except Exception:
                                filtered.append(r)
                        else:
                            filtered.append(r)  # Нет текста для определения — пропускаем
                    else:
                        # lang определён но не в allowed — пробуем детекцию по тексту
                        sample = (r.abstract or r.title or "")[:3000]
                        if sample:
                            try:
                                detected = _normalize_lang_code(detect_language(sample))
                                if detected in allowed:
                                    r.metadata["language"] = detected
                                    filtered.append(r)
                                    continue
                            except Exception:
                                pass
                        # Если всё равно не удалось — пропускаем
                        print(f"[arxiv] language filter: skipping '{(r.title or '')[:50]}' (lang={lang}, not in {allowed})", flush=True)
                if len(filtered) != len(results):
                    logger.info("arXiv: language filter %s -> %s (allowed: %s)", len(results), len(filtered), allowed)
                results = filtered[:max_results]
            else:
                results = results[:max_results]
        else:
            results = results[:max_results]

        logger.info("arXiv: query '%s' -> %s results", query_clean[:80], len(results))
        return results

    async def fetch_article(self, url: str) -> Optional[SourceResult]:
        """Загрузка препринта по URL arxiv.org/abs/ID или по ID (например 2301.12345)."""
        url = url.strip()
        arxiv_id = None
        if "arxiv.org/abs/" in url:
            arxiv_id = url.split("arxiv.org/abs/")[-1].split("?")[0].rstrip("/")
        elif re.match(r"^\d{4}\.\d{4,5}(v\d+)?$", url):
            arxiv_id = url
        if not arxiv_id:
            return None
        # Убираем версию для id_list (API возвращает последнюю)
        arxiv_id = re.sub(r"v\d+$", "", arxiv_id)
        params = {"id_list": arxiv_id}
        api_url = f"{self._base}?{urlencode(params, doseq=True)}"
        try:
            xml_text = await self._get(api_url)
        except SourceTemporarilyUnavailableError:
            raise
        except Exception as e:
            logger.warning("arXiv fetch_article %s: %s", url, e)
            return None
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return None
        entries = _find_all_elems(root, "entry", _ATOM_NAMESPACES)
        for entry in entries:
            res = _entry_to_source_result(entry, source_name=self.name)
            if res:
                return res
        return None

    def to_rag_document(
        self,
        result: SourceResult,
        *,
        chunk_size_min: int = 500,
        chunk_size_max: int = 2000,
        overlap: int = 100,
    ) -> RAGDocument:
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
        metadata["is_dissertation"] = is_dissertation(title_abstract)

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
            source="arXiv",
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


register_source("arxiv", ArxivSource)
