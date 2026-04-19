"""Источник: CiNii Dissertations (OpenSearch API). Японские диссертации, метаданные и ссылки на полные тексты."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
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

CINII_SEARCH_BASE = "https://ci.nii.ac.jp/d/search"
CINII_DETAIL_BASE = "https://ci.nii.ac.jp"
DEFAULT_USER_AGENT = "RAG-Collector/1.0 (https://github.com/rag-collector; mailto:support@example.org)"


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


def _json_value(obj: Any) -> str:
    """Из JSON-LD: @value или строка."""
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj.strip()
    if isinstance(obj, dict) and "@value" in obj:
        return (obj["@value"] or "").strip()
    return str(obj).strip()


def _json_id(obj: Any) -> Optional[str]:
    """Из JSON-LD: @id или строка URL."""
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj.strip() or None
    if isinstance(obj, dict) and "@id" in obj:
        return (obj["@id"] or "").strip() or None
    return None


def _parse_list(obj: Any) -> list:
    """Нормализует в список (один элемент или массив)."""
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    return [obj]


def _item_to_source_result(item: dict, source_name: str = "CiNii Dissertations") -> Optional[SourceResult]:
    """Парсит один элемент из поиска CiNii (JSON-LD item) в SourceResult."""
    if not item or not isinstance(item, dict):
        return None
    title = _json_value(item.get("title"))
    link_obj = item.get("link")
    url = _json_id(link_obj) or (link_obj if isinstance(link_obj, str) else "") or ""
    if not url and item.get("@id"):
        url = item["@id"] if isinstance(item["@id"], str) else item["@id"].get("@id", "")

    creators = item.get("dc:creator") or item.get("creator") or []
    authors = [_json_value(c) for c in _parse_list(creators) if _json_value(c)]

    date_raw = _json_value(item.get("dc:date"))
    pub_date = None
    if date_raw:
        m = re.match(r"(\d{4})", date_raw)
        if m:
            pub_date = f"{m.group(1)}-01-01"

    publisher = _json_value(item.get("dc:publisher") or item.get("publisher"))
    degree_name = _json_value(item.get("ndl:degreeName") or item.get("degreeName"))
    grant_id = _json_value(item.get("ndl:dissertationNumber") or item.get("dissertationNumber"))

    pdf_url = None
    sources = _parse_list(item.get("dc:source") or item.get("source"))
    for s in sources:
        if isinstance(s, dict):
            href = _json_id(s)
            if href and ("pdf" in (s.get("dc:title") or s.get("title") or "").lower() or "ndl.go.jp" in (href or "")):
                pdf_url = href
                break
            if href and not pdf_url:
                pdf_url = href
        elif isinstance(s, str) and s.startswith("http"):
            pdf_url = s
            break

    meta: dict[str, Any] = {
        "publisher": publisher,
        "degree_name": degree_name,
        "dissertation_number": grant_id,
    }
    abstract = _json_value(item.get("dc:description") or item.get("description"))
    if abstract:
        meta["description"] = abstract

    return SourceResult(
        title=title or "Без названия",
        url=url,
        authors=authors,
        date=pub_date,
        doi=None,
        abstract=abstract,
        full_text="",
        pdf_url=pdf_url,
        metadata=meta,
        source_name=source_name,
    )


class CiniiSource(BaseSource):
    """Поиск диссертаций в CiNii Dissertations (OpenSearch, JSON-LD)."""

    name = "cinii"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._timeout = 30
        self._request_delay = 1.0
        try:
            import yaml
            root = Path(__file__).resolve().parent.parent.parent
            cfg_path = root / "config" / "settings.yaml"
            if cfg_path.exists():
                with open(cfg_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                cfg = (data.get("sources") or {}).get("cinii") or {}
                self._timeout = int(cfg.get("timeout", self._timeout))
                self._request_delay = float(cfg.get("request_delay", self._request_delay))
        except Exception:
            pass

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/ld+json, application/json"},
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
            if resp.status in (403, 429, 503):
                raise SourceTemporarilyUnavailableError(
                    f"CiNii API unavailable (status {resp.status}): may be blocked or rate limited",
                    source_name=self.name,
                )
            resp.raise_for_status()
            return await resp.text()

    async def _get_json(self, url: str) -> dict:
        text = await self._get(url)
        import json
        return json.loads(text)

    async def search(
        self,
        query: str,
        *,
        max_results: int = 50,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        languages: Optional[list[str]] = None,
    ) -> list[SourceResult]:
        """Поиск диссертаций в CiNii по ключевым словам и году (year_from/year_to)."""
        if not (query or "").strip():
            return []
        query_clean = query.strip()[:500]
        # Проверяем наличие кириллицы в запросе
        has_cyrillic = any('\u0400' <= char <= '\u04FF' for char in query_clean)
        if has_cyrillic:
            logger.warning("CiNii: query contains Cyrillic characters. CiNii API does not support Cyrillic, will return 0 results.")
            print(f"[cinii] WARNING: Query contains Cyrillic characters. CiNii does not support Cyrillic, will return 0 results.", flush=True)
        logger.info("CiNii: search query='%s', date_from=%s, date_to=%s, max_results=%s", 
                    query_clean[:100], date_from, date_to, max_results)
        results: list[SourceResult] = []
        count_per_page = min(20, max(max_results, 1))
        page = 1
        total_collected = 0

        while total_collected < max_results:
            params: dict[str, Any] = {
                "q": query_clean,
                "format": "json",
                "count": count_per_page,
                "p": page,
                "sortorder": 5,
            }
            if date_from:
                y = date_from[:4] if len(date_from) >= 4 else ""
                if y.isdigit():
                    params["year_from"] = y
            if date_to:
                y = date_to[:4] if len(date_to) >= 4 else ""
                if y.isdigit():
                    params["year_to"] = y

            url = f"{CINII_SEARCH_BASE}?{urlencode(params, doseq=True)}"
            print(f"[cinii] Requesting URL: {url}", flush=True)
            try:
                data = await self._get_json(url)
                await asyncio.sleep(self._request_delay)
            except SourceTemporarilyUnavailableError:
                raise
            except Exception as e:
                logger.exception("CiNii search failed: %s", e)
                print(f"[cinii] Exception during search: {e}", flush=True)
                break

            # CiNii API может возвращать items в @graph[0].items или напрямую в data.items
            items = None
            graph = data.get("@graph") or data.get("graph")
            if graph:
                channel = graph[0] if isinstance(graph, list) else graph
                if isinstance(channel, dict):
                    items = channel.get("items")
            # Fallback: items на верхнем уровне (новый формат CiNii API)
            if items is None:
                items = data.get("items")
            if items is None:
                total_in_response = data.get("opensearch:totalResults", "?")
                print(f"[cinii] No items found in response (totalResults={total_in_response}). Response keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}", flush=True)
                break
            item_list = items if isinstance(items, list) else [items]
            for it in item_list:
                if total_collected >= max_results:
                    break
                res = _item_to_source_result(it, source_name=self.name)
                if res and res.url:
                    res.metadata["_original_query"] = query_clean
                    results.append(res)
                    total_collected += 1
            if len(item_list) < count_per_page:
                break
            page += 1

        if languages:
            allowed = {_normalize_lang_code(l) for l in languages if l and isinstance(l, str)}
            if allowed:
                filtered = []
                for r in results:
                    lang = (r.metadata or {}).get("language")
                    if lang and lang in allowed:
                        filtered.append(r)
                    elif not lang:
                        # CiNii — японская база, многие документы без метки языка.
                        # Пробуем определить язык по тексту; если не удаётся — пропускаем
                        if r.abstract or r.title:
                            try:
                                detected = _normalize_lang_code(detect_language((r.abstract or r.title or "")[:3000]))
                                r.metadata["language"] = detected
                                if detected in allowed:
                                    filtered.append(r)
                                    continue
                            except Exception:
                                pass
                        # Если язык не определён и запрос на английском — пропускаем документ
                        # (CiNii содержит диссертации на разных языках, в т.ч. английском)
                        filtered.append(r)
                    elif lang not in allowed:
                        # Язык определён, но не в списке — пробуем определить по тексту
                        if r.abstract or r.title:
                            try:
                                detected = _normalize_lang_code(detect_language((r.abstract or r.title or "")[:3000]))
                                if detected in allowed:
                                    r.metadata["language"] = detected
                                    filtered.append(r)
                                    continue
                            except Exception:
                                pass
                if len(filtered) != len(results):
                    logger.info("CiNii: language filter %s -> %s (allowed: %s)", len(results), len(filtered), allowed)
                results = filtered[:max_results]
            else:
                results = results[:max_results]
        else:
            results = results[:max_results]

        logger.info("CiNii Dissertations: query '%s' -> %s results", query_clean[:80], len(results))
        return results

    async def fetch_article(self, url: str) -> Optional[SourceResult]:
        """Загрузка метаданных диссертации по URL ci.nii.ac.jp/d/NAID или ci.nii.ac.jp/naid/NAID."""
        url = url.strip()
        naid = None
        if "ci.nii.ac.jp/d/" in url:
            naid = url.split("ci.nii.ac.jp/d/")[-1].split("?")[0].rstrip("/").split("#")[0]
        elif "ci.nii.ac.jp/naid/" in url:
            naid = url.split("ci.nii.ac.jp/naid/")[-1].split("?")[0].rstrip("/").split(".")[0]
        if not naid or not naid.isdigit():
            return None
        for json_url in (f"https://ci.nii.ac.jp/naid/{naid}.json", f"https://ci.nii.ac.jp/d/{naid}.json"):
            try:
                data = await self._get_json(json_url)
                break
            except SourceTemporarilyUnavailableError:
                raise
            except Exception as e:
                logger.debug("CiNii fetch %s: %s", json_url, e)
                continue
        else:
            logger.warning("CiNii fetch_article %s: no JSON available", url)
            return None

        graph = data.get("@graph")
        if not graph or not isinstance(graph, list):
            return None
        article = graph[0] if graph else {}
        if not isinstance(article, dict):
            return None
        title_arr = article.get("dc:title") or article.get("title")
        title = _json_value(title_arr[0] if isinstance(title_arr, list) and title_arr else title_arr)
        creators = article.get("dc:creator") or article.get("foaf:maker")
        authors = []
        for c in _parse_list(creators):
            if isinstance(c, dict):
                name_arr = c.get("foaf:name") or c.get("name")
                authors.append(_json_value(name_arr[0] if isinstance(name_arr, list) and name_arr else name_arr))
            else:
                authors.append(_json_value(c))
        date_raw = _json_value(article.get("dc:date") or article.get("ndl:dateGranted"))
        pub_date = f"{date_raw[:4]}-01-01" if date_raw and len(date_raw) >= 4 and date_raw[:4].isdigit() else None
        desc_arr = article.get("dc:description") or article.get("description")
        abstract = _json_value(desc_arr[0] if isinstance(desc_arr, list) and desc_arr else desc_arr)
        publisher_arr = article.get("dc:publisher") or article.get("publisher")
        publisher = _json_value(publisher_arr[0] if isinstance(publisher_arr, list) and publisher_arr else publisher_arr)
        detail_url = f"https://ci.nii.ac.jp/d/{naid}"
        sources = _parse_list(article.get("dc:source") or article.get("source"))
        pdf_url = None
        for s in sources:
            if isinstance(s, dict):
                href = _json_id(s)
                if href:
                    pdf_url = href
                    break
            elif isinstance(s, str) and s.startswith("http"):
                pdf_url = s
                break
        meta = {"publisher": publisher, "naid": naid, "degree_name": _json_value(article.get("ndl:degreeName"))}
        return SourceResult(
            title=title or "Без названия",
            url=detail_url,
            authors=authors,
            date=pub_date,
            doi=_json_value(article.get("prism:doi")),
            abstract=abstract,
            full_text="",
            pdf_url=pdf_url,
            metadata=meta,
            source_name="CiNii Dissertations",
        )

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
        if not language and (result.abstract or result.title or "").strip():
            try:
                language = _normalize_lang_code(detect_language((result.abstract or result.title or "")[:5000]))
            except Exception:
                pass
        if not language:
            language = "ja"

        file_access = "abstract_only"
        if result.pdf_url:
            file_access = "pdf_direct_link"

        metadata = {k: v for k, v in (result.metadata or {}).items() if not k.startswith("_")}
        title_abstract = (result.title or "") + " " + (result.abstract or "")
        metadata["document_type"] = document_type_from_text(result.title or "", result.abstract or "", "")
        metadata["is_dissertation"] = True

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
            source="CiNii Dissertations",
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


register_source("cinii", CiniiSource)
