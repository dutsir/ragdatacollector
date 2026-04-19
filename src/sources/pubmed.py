"""Источник: PubMed (NCBI E-utilities). Биомедицинские статьи, метаданные и аннотации."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import xml.etree.ElementTree as ET
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

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
# NCBI рекомендует tool и email при частых запросах
DEFAULT_USER_AGENT = "RAG-Collector/1.0 (https://github.com/rag-collector; mailto:support@example.org)"

# PubMed XML может приходить с namespace (http://www.ncbi.nlm.nih.gov)
NS = {"pubmed": "http://www.ncbi.nlm.nih.gov"}


def _find_all_by_localname(parent, local_name: str):
    """Находит все дочерние элементы по локальному имени (игнорируя namespace)."""
    out = []
    for e in parent.iter():
        tag = e.tag
        if tag == local_name or (isinstance(tag, str) and tag.endswith("}" + local_name)):
            out.append(e)
    return out


def _find_one(parent, local_name: str):
    """Находит первый дочерний элемент по локальному имени."""
    for e in parent.iter():
        tag = e.tag
        if tag == local_name or (isinstance(tag, str) and tag.endswith("}" + local_name)):
            return e
    return None


def _elem_text(elem):
    if elem is None:
        return ""
    return (elem.text or "").strip() or "".join(elem.itertext()).strip()


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


def _parse_pubdate(elem) -> Optional[str]:
    """Из MedlineCitation/Article/Journal/JournalIssue/PubDate или ArticleDate -> YYYY-MM-DD."""
    if elem is None:
        return None
    year = elem.find("Year")
    med = elem.find("MedlineDate")
    if year is not None and year.text:
        y = year.text.strip()
        month = elem.find("Month")
        day = elem.find("Day")
        m = (month.text or "1").strip()
        d = (day.text or "1").strip()
        if len(m) == 3:  # Jan, Feb, ...
            months = "jan feb mar apr may jun jul aug sep oct nov dec".split()
            try:
                m = str(months.index(m.lower()) + 1).zfill(2)
            except ValueError:
                m = "01"
        elif len(m) == 1:
            m = m.zfill(2)
        if len(d) == 1:
            d = d.zfill(2)
        return f"{y}-{m}-{d}"
    if med is not None and med.text:
        # "2024" or "2024 Jan"
        t = med.text.strip()[:10]
        if len(t) >= 4 and t[:4].isdigit():
            return t[:4] + "-01-01"
    return None


def _parse_article_xml(article_elem) -> Optional[SourceResult]:
    """Парсит один PubmedArticle в SourceResult (устойчиво к namespace в XML)."""
    try:
        medline = _find_one(article_elem, "MedlineCitation")
        if medline is None:
            medline = article_elem
        article = _find_one(medline, "Article")
        if article is None:
            return None

        title_elem = _find_one(article, "ArticleTitle")
        title = _elem_text(title_elem)

        abstract_parts = []
        abstract_elem = _find_one(article, "Abstract")
        if abstract_elem is not None:
            for abst in abstract_elem:
                if abst.tag.endswith("}AbstractText") or abst.tag == "AbstractText":
                    text = _elem_text(abst)
                    if text:
                        abstract_parts.append(text)
        abstract = " ".join(abstract_parts) if abstract_parts else ""

        authors = []
        author_list = _find_one(article, "AuthorList")
        if author_list is not None:
            for a in author_list:
                if a.tag.endswith("}Author") or a.tag == "Author":
                    last = _find_one(a, "LastName")
                    fore = _find_one(a, "ForeName")
                    last_s = _elem_text(last)
                    fore_s = _elem_text(fore)
                    name = f"{fore_s} {last_s}".strip() or last_s
                    if name:
                        authors.append(name)

        pub_date = None
        journal = _find_one(article, "Journal")
        if journal is not None:
            ji = _find_one(journal, "JournalIssue")
            if ji is not None:
                pub_date = _parse_pubdate(_find_one(ji, "PubDate"))
        if not pub_date:
            pub_date = _parse_pubdate(_find_one(article, "ArticleDate"))

        pmid = None
        doi = None
        pmc_id = None
        pubmed_data = _find_one(article_elem, "PubmedData")
        if pubmed_data is None:
            pubmed_data = _find_one(medline, "PubmedData")
        if pubmed_data is not None:
            id_list = _find_one(pubmed_data, "ArticleIdList")
            if id_list is not None:
                for aid in id_list:
                    if aid.tag.endswith("}ArticleId") or aid.tag == "ArticleId":
                        id_type = (aid.get("IdType") or "").strip().lower()
                        val = _elem_text(aid)
                        if id_type == "pubmed":
                            pmid = val
                        elif id_type == "doi":
                            doi = val
                        elif id_type == "pmc":
                            pmc_id = val.lstrip("PMC")

        if not pmid:
            pmid_elem = _find_one(medline, "PMID")
            if pmid_elem is not None:
                pmid = _elem_text(pmid_elem)

        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else ""
        pdf_url = None
        if pmc_id:
            pdf_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/pdf/"

        meta: dict[str, Any] = {
            "pmid": pmid,
            "pmc": pmc_id,
            "journal": "",
        }
        if journal is not None:
            title_ja = _find_one(journal, "Title")
            if title_ja is not None:
                meta["journal"] = _elem_text(title_ja)

        return SourceResult(
            title=title,
            url=url,
            authors=authors,
            date=pub_date,
            doi=doi,
            abstract=abstract,
            full_text="",
            pdf_url=pdf_url,
            metadata=meta,
            source_name="PubMed",
        )
    except Exception as e:
        logger.debug("PubMed parse article: %s", e)
        return None


class PubMedSource(BaseSource):
    """Поиск и загрузка статей через NCBI E-utilities (PubMed)."""

    name = "pubmed"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._timeout = 60  # NCBI за рубежом может отвечать медленно; увеличить при таймаутах
        self._connect_timeout = 25  # отдельный лимит на установку соединения (снижает «таймаут семафора» на Windows)
        self._request_delay = 0.4  # NCBI: не более 3 запросов в секунду
        self._max_retries = 3  # повторы при сетевых ошибках (ClientConnectorError, Timeout)
        self._tool = "RAG-Collector"
        self._email = ""
        try:
            import yaml
            root = Path(__file__).resolve().parent.parent.parent
            cfg_path = root / "config" / "settings.yaml"
            if cfg_path.exists():
                with open(cfg_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                cfg = (data.get("sources") or {}).get("pubmed") or {}
                self._timeout = int(cfg.get("timeout", self._timeout))
                self._connect_timeout = int(cfg.get("connect_timeout", self._connect_timeout))
                self._request_delay = float(cfg.get("request_delay", self._request_delay))
                self._max_retries = max(1, min(5, int(cfg.get("max_retries", self._max_retries))))
                self._email = (cfg.get("email") or "").strip()
                if cfg.get("tool"):
                    self._tool = cfg["tool"]
        except Exception:
            pass

    def _base_params(self) -> dict:
        params = {"tool": self._tool}
        if self._email:
            params["email"] = self._email
        return params

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(
                        connect=self._connect_timeout,
                        total=self._timeout,
                    ),
                )
            return self._session

    async def close(self):
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None

    async def _get(self, url: str) -> str:
        session = await self._get_session()
        timeout = aiohttp.ClientTimeout(connect=self._connect_timeout, total=self._timeout)
        last_exc = None
        for attempt in range(1, self._max_retries + 1):
            try:
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 429 or resp.status == 503:
                        raise SourceTemporarilyUnavailableError(
                            f"PubMed/NCBI rate limit or unavailable: {resp.status}",
                            source_name=self.name,
                        )
                    resp.raise_for_status()
                    return await resp.text()
            except SourceTemporarilyUnavailableError:
                raise
            except (aiohttp.ClientConnectorError, aiohttp.ServerTimeoutError, asyncio.TimeoutError, OSError) as e:
                last_exc = e
                if attempt < self._max_retries:
                    delay = 2 * attempt
                    logger.warning(
                        "PubMed connection attempt %s/%s failed (%s), retry in %s s: %s",
                        attempt,
                        self._max_retries,
                        type(e).__name__,
                        delay,
                        e,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.exception(
                        "PubMed connection failed after %s attempts: %s",
                        self._max_retries,
                        e,
                    )
                    raise

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
        """Поиск в PubMed по ключевым словам с фильтром по дате публикации."""
        if not (query or "").strip():
            print("[pubmed] Empty query, returning empty results", flush=True)
            return []
        query_clean = query.strip()[:500]
        # Проверяем наличие кириллицы в запросе
        has_cyrillic = any('\u0400' <= char <= '\u04FF' for char in query_clean)
        if has_cyrillic:
            logger.warning("PubMed: query contains Cyrillic characters. PubMed API may not handle Cyrillic queries well.")
            print(f"[pubmed] WARNING: Query contains Cyrillic characters. PubMed may return 0 or irrelevant results.", flush=True)
        print(f"[pubmed] Starting search: query='{query_clean[:100]}', max_results={max_results}, date_from={date_from}, date_to={date_to}", flush=True)
        params: dict[str, Any] = {
            **self._base_params(),
            "db": "pubmed",
            "term": query_clean,
            "retmax": min(max_results, 500),
            "retmode": "json",
            "sort": "relevance",
        }
        if date_from or date_to:
            # mindate/maxdate в формате YYYY/MM/DD; NCBI требует оба параметра вместе
            params["datetype"] = "pdat"
            params["mindate"] = (date_from or "1900/01/01").replace("-", "/")[:10]
            params["maxdate"] = (date_to or "2099/12/31").replace("-", "/")[:10]

        url = f"{EUTILS_BASE}/esearch.fcgi?{urlencode(params, doseq=True)}"
        print(f"[pubmed] Requesting PubMed API: {url[:200]}", flush=True)
        try:
            data = await self._get_json(url)
            print(f"[pubmed] Received response from PubMed API", flush=True)
        except SourceTemporarilyUnavailableError as e:
            print(f"[pubmed] SourceTemporarilyUnavailableError: {e}", flush=True)
            raise
        except Exception as e:
            logger.exception("PubMed esearch failed: %s", e)
            print(f"[pubmed] Exception during esearch: {e}", flush=True)
            import traceback
            print(f"[pubmed] Traceback: {traceback.format_exc()}", flush=True)
            return []

        esearch_result = data.get("esearchresult") or {}
        id_list = esearch_result.get("idlist") or []
        total_found = esearch_result.get("count", "0")
        print(f"[pubmed] PubMed esearch returned {len(id_list)} IDs (total found: {total_found})", flush=True)
        if not id_list:
            logger.info("PubMed: query '%s' -> 0 ids", query_clean[:80])
            print(f"[pubmed] No IDs found for query '{query_clean[:80]}' (total found: {total_found})", flush=True)
            return []

        results: list[SourceResult] = []
        batch_size = 100
        for i in range(0, len(id_list), batch_size):
            batch_ids = id_list[i : i + batch_size]
            ids_param = ",".join(batch_ids)
            fetch_params = {
                **self._base_params(),
                "db": "pubmed",
                "id": ids_param,
                "retmode": "xml",
            }
            fetch_url = f"{EUTILS_BASE}/efetch.fcgi?{urlencode(fetch_params, doseq=True)}"
            try:
                xml_text = await self._get(fetch_url)
                await asyncio.sleep(self._request_delay)
            except SourceTemporarilyUnavailableError:
                raise
            except Exception as e:
                logger.warning("PubMed efetch batch failed: %s", e)
                continue

            try:
                root = ET.fromstring(xml_text)
            except ET.ParseError as e:
                logger.warning("PubMed XML parse error: %s", e)
                continue

            for article in _find_all_by_localname(root, "PubmedArticle"):
                res = _parse_article_xml(article)
                if res:
                    res.metadata["_original_query"] = query_clean
                    results.append(res)

        results = results[:max_results]

        if languages:
            allowed = {_normalize_lang_code(l) for l in languages if l and isinstance(l, str)}
            if allowed:
                filtered = []
                for r in results:
                    lang = (r.metadata or {}).get("language")
                    if lang and lang in allowed:
                        filtered.append(r)
                    elif not lang and (r.abstract or r.title):
                        try:
                            lang = _normalize_lang_code(detect_language((r.abstract or r.title or "")[:3000]))
                            r.metadata["language"] = lang
                            if lang in allowed:
                                filtered.append(r)
                        except Exception:
                            pass
                if len(filtered) != len(results):
                    logger.info("PubMed: language filter %s -> %s (allowed: %s)", len(results), len(filtered), allowed)
                results = filtered[:max_results]
            else:
                results = results[:max_results]

        logger.info("PubMed: query '%s' -> %s results", query_clean[:80], len(results))
        print(f"[pubmed] Final results: {len(results)} documents", flush=True)
        return results

    async def fetch_article(self, url: str) -> Optional[SourceResult]:
        """Загрузка статьи по URL pubmed.ncbi.nlm.nih.gov/PMID или по PMID."""
        url = url.strip()
        pmid = None
        if "pubmed.ncbi.nlm.nih.gov/" in url:
            pmid = url.split("pubmed.ncbi.nlm.nih.gov/")[-1].split("?")[0].rstrip("/").split("/")[0]
        if not pmid or not pmid.isdigit():
            return None
        params = {**self._base_params(), "db": "pubmed", "id": pmid, "retmode": "xml"}
        fetch_url = f"{EUTILS_BASE}/efetch.fcgi?{urlencode(params, doseq=True)}"
        try:
            xml_text = await self._get(fetch_url)
        except SourceTemporarilyUnavailableError:
            raise
        except Exception as e:
            logger.warning("PubMed fetch_article %s: %s", url, e)
            return None
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return None
        for article in _find_all_by_localname(root, "PubmedArticle"):
            res = _parse_article_xml(article)
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
            source="PubMed",
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


register_source("pubmed", PubMedSource)
