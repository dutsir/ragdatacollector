"""Источник: CrossRef (REST API, публичный). Метаданные и аннотации статей по DOI."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlencode
import xml.etree.ElementTree as ET

import aiohttp

from ..models.document import FileRef, ProcessingInfo, RAGDocument
from ..processing import (
    chunk_text,
    compute_chunk_info,
    compute_validation_score,
    detect_language,
    document_type_from_text,
    extract_metadata_from_pdf_text,
    filter_and_clean_chunks,
    is_dissertation,
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

CROSSREF_API_BASE = "https://api.crossref.org"
UNPAYWALL_API_BASE = "https://api.unpaywall.org"
# Polite pool: User-Agent с mailto обязателен (https://www.crossref.org/documentation/retrieve-metadata/rest-api/tips/)
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


def _strip_html_abstract(raw: str) -> str:
    """Убирает JATS/HTML из аннотации CrossRef."""
    if not raw or not isinstance(raw, str):
        return ""
    s = re.sub(r"<[^>]+>", " ", raw)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_date(item: dict) -> Optional[str]:
    """Из date-parts [[year, month, day]] формирует YYYY-MM-DD."""
    for key in ("issued", "published", "published-print", "published-online", "created"):
        parts = (item.get(key) or {}).get("date-parts")
        if not parts or not parts[0]:
            continue
        p = parts[0]
        y = p[0] if len(p) > 0 else None
        m = p[1] if len(p) > 1 else 1
        d = p[2] if len(p) > 2 else 1
        if y is None:
            continue
        return f"{y:04d}-{m:02d}-{d:02d}"
    return None


def _parse_authors(authors: list) -> list[str]:
    out = []
    for a in authors or []:
        if not isinstance(a, dict):
            continue
        given = (a.get("given") or "").strip()
        family = (a.get("family") or "").strip()
        name = f"{given} {family}".strip() or family or given
        if name:
            out.append(name)
    return out


def _extract_full_text_links(links: list) -> dict[str, Any]:
    """
    Извлекает ссылки на полные тексты из поля link с приоритетами.
    Возвращает словарь с типами контента и URL.
    Приоритет: PDF > text/plain > text/xml > другие текстовые форматы.
    """
    result = {
        "pdf_url": None,
        "text_plain_url": None,
        "text_xml_url": None,
        "other_text_urls": [],
        "all_links": [],  # Все ссылки с метаданными для отладки
    }
    
    if not links:
        return result
    
    # Приоритеты типов контента
    priority_order = [
        ("application/pdf", "pdf"),
        ("text/plain", "text_plain"),
        ("text/xml", "text_xml"),
        ("application/xml", "text_xml"),
    ]
    
    for ln in links:
        if not isinstance(ln, dict):
            continue
        
        url = (ln.get("URL") or "").strip()
        if not url:
            continue
        
        content_type = (ln.get("content-type") or "").strip().lower()
        content_version = (ln.get("content-version") or "").strip()
        intended_app = (ln.get("intended-application") or "").strip().lower()
        
        link_info = {
            "url": url,
            "content_type": content_type,
            "content_version": content_version,
            "intended_application": intended_app,
        }
        result["all_links"].append(link_info)
        
        # Проверяем по приоритетам
        found = False
        for ct_pattern, key in priority_order:
            if ct_pattern in content_type:
                if key == "pdf" and not result["pdf_url"]:
                    result["pdf_url"] = url
                    found = True
                    break
                elif key == "text_plain" and not result["text_plain_url"]:
                    result["text_plain_url"] = url
                    found = True
                    break
                elif key == "text_xml" and not result["text_xml_url"]:
                    result["text_xml_url"] = url
                    found = True
                    break
        
        # Если не нашли по приоритетам, но это текстовый формат
        if not found and content_type and any(
            t in content_type for t in ["text/", "application/xml", "application/json"]
        ):
            if url not in result["other_text_urls"]:
                result["other_text_urls"].append(url)
    
    return result


def _pdf_url_from_links(links: list) -> Optional[str]:
    """Ищет в link[] URL с content-type application/pdf (обратная совместимость)."""
    full_text_info = _extract_full_text_links(links)
    return full_text_info.get("pdf_url")


def _normalize_lang_code(lang: str) -> str:
    """Нормализует код языка до 2 букв (en-GB -> en, mk -> mk)."""
    if not lang or not isinstance(lang, str):
        return ""
    s = lang.strip().lower().split("-")[0][:2]
    return s


def _language_from_work(work: dict) -> Optional[str]:
    """Извлекает код языка из ответа CrossRef (поле language)."""
    raw = work.get("language")
    if raw is None:
        return None
    if isinstance(raw, str):
        return _normalize_lang_code(raw) or None
    if isinstance(raw, list) and raw:
        first = raw[0]
        if isinstance(first, str):
            return _normalize_lang_code(first) or None
    return None


def _calculate_relevance_score(result: SourceResult, query: str) -> float:
    """
    Улучшенный расчет релевантности с учетом контекста и штрафов за нерелевантные термины.
    """
    if not query or not query.strip():
        return 0.0
    
    import re
    
    query_lower = query.lower().strip()
    # Извлекаем слова: для кириллицы минимум 3 символа, для латиницы 2
    query_words = []
    for w in query_lower.split():
        w_stripped = w.strip()
        # Убираем пунктуацию
        w_clean = re.sub(r'[^\w]', '', w_stripped)
        if len(w_clean) >= 2:  # Снижаем минимальную длину для лучшего охвата
            query_words.append(w_clean)
    
    if not query_words:
        logger.debug(f"CrossRef: No valid query words extracted from '{query}'")
        return 0.0
    
    title = (result.title or "").lower()
    abstract = (result.abstract or "").lower()
    full_text = (result.full_text or "").lower()
    
    # Нормализация для лучшего сопоставления (убираем пунктуацию, нормализуем пробелы)
    title_normalized = re.sub(r'[^\w\s]', ' ', title)
    abstract_normalized = re.sub(r'[^\w\s]', ' ', abstract)
    
    # Проверяем все ключевые слова
    all_matches = 0
    title_matches = 0
    abstract_matches = 0
    
    # Синонимы для типичных запросов (чтобы "USA" находил "United States" и т.д.)
    _synonyms = {
        "usa": ["usa", "u.s.a.", "united states", " us "],
        "сша": ["сша", "соединенные штаты", "соединённые штаты"],
    }
    
    for word in query_words:
        word_lower = word.lower()
        # Проверяем прямое вхождение или синонимы
        def _word_in_text(txt: str, w: str) -> bool:
            if not txt:
                return False
            if w in txt:
                return True
            for key, variants in _synonyms.items():
                if key == w:
                    return any(v in txt for v in variants)
            return False
        
        if _word_in_text(title_normalized, word_lower):
            title_matches += 1
            all_matches += 1
        elif _word_in_text(abstract_normalized, word_lower):
            abstract_matches += 1
            all_matches += 1
        elif full_text:
            full_text_normalized = re.sub(r'[^\w\s]', ' ', full_text)
            if _word_in_text(full_text_normalized, word_lower):
                all_matches += 1
    
    # Расчет релевантности с весами
    if not query_words:
        return 0.0
    
    # Базовый расчет: процент совпадений
    base_relevance = all_matches / len(query_words)
    
    # Бонус за совпадения в заголовке (более важный)
    title_bonus = (title_matches / len(query_words)) * 0.5 if title_matches > 0 else 0
    
    # Бонус за совпадения в аннотации
    abstract_bonus = (abstract_matches / len(query_words)) * 0.2 if abstract_matches > 0 else 0
    
    relevance = base_relevance * 0.3 + title_bonus + abstract_bonus
    
    # Штраф за нерелевантные документы (например, Каспий для запроса про США)
    penalty_terms = {
        "сша": ["каспий", "сербия", "балкан", "азия", "ближний восток", "каспийский"],
        "usa": ["caspian", "serbia", "balkan", "asia", "middle east"],
        "геополитика сша": ["каспий", "каспийский", "балкан", "сербия"],
        "us geopolitics": ["caspian", "balkan", "serbia"],
    }
    
    penalty_applied = False
    for keyword, penalty_list in penalty_terms.items():
        if keyword in query_lower:
            for penalty in penalty_list:
                if penalty in title or penalty in abstract:
                    relevance *= 0.3  # Сильный штраф за нерелевантные термины
                    penalty_applied = True
                    logger.debug(f"Relevance penalty applied: '{penalty}' found for query '{query[:50]}'")
                    break
            if penalty_applied:
                break
    
    final_score = min(1.0, max(0.0, relevance))
    
    # Детальное логирование для отладки (только для первых документов)
    if hasattr(_calculate_relevance_score, '_debug_count'):
        _calculate_relevance_score._debug_count += 1
    else:
        _calculate_relevance_score._debug_count = 1
    
    if _calculate_relevance_score._debug_count <= 5:
        logger.info(
            f"Relevance calculation: query='{query}', words={query_words}, "
            f"title='{result.title[:50] if result.title else 'N/A'}...', "
            f"title_matches={title_matches}, abstract_matches={abstract_matches}, "
            f"all_matches={all_matches}/{len(query_words)}, base={base_relevance:.2f}, "
            f"title_bonus={title_bonus:.2f}, abstract_bonus={abstract_bonus:.2f}, "
            f"final={final_score:.3f}, penalty_applied={penalty_applied}"
        )
    
    return final_score


def _detect_language_from_text(text: str) -> str:
    """
    Определяет язык из текста с учетом особенностей и коррекций.
    Исправляет распространенные ошибки детектора.
    """
    if not text or len(text.strip()) < 100:
        return "en"
    
    try:
        detected = detect_language(text[:5000])  # Ограничиваем для скорости
        normalized = _normalize_lang_code(detected) if detected else "en"
        
        # Карта исправления распространенных ошибок детектора
        # Проверяем наличие специфических символов для точного определения
        if normalized in ["sh", "hr", "bs"] and any(chr in text for chr in ["ћ", "ђ", "љ", "њ", "џ"]):
            # Сербские специфические символы
            return "sr"
        elif normalized in ["uk", "be"] and any(chr in text for chr in ["ы", "э", "ъ"]):
            # Русские специфические символы
            return "ru"
        
        return normalized or "en"
    except Exception:
        return "en"


def _get_result_language(result: SourceResult) -> str:
    """
    Определяет язык документа: сначала из метаданных, затем детектором.
    Приоритет: метаданные API > заголовок+аннотация > полный текст.
    """
    # 1. Язык из метаданных CrossRef (наиболее надежный)
    meta_lang = (result.metadata or {}).get("language")
    if meta_lang:
        if isinstance(meta_lang, str):
            normalized = _normalize_lang_code(meta_lang)
            if normalized and normalized != "und":  # "und" = undefined
                return normalized
        elif isinstance(meta_lang, list) and meta_lang:
            first = meta_lang[0]
            if isinstance(first, str):
                normalized = _normalize_lang_code(first)
                if normalized and normalized != "und":
                    return normalized
    
    # 2. Детекция по заголовку и аннотации
    text = ((result.title or "") + " " + (result.abstract or "")).strip()
    if len(text) >= 50:  # Увеличим минимальную длину для надежности
        try:
            detected_lang = _detect_language_from_text(text)
            if detected_lang and detected_lang != "und":
                return detected_lang
        except Exception:
            pass
    
    # 3. Если полный текст уже загружен, пробуем определить по нему
    if result.full_text and len(result.full_text) > 200:
        try:
            # Берем только начало текста для скорости
            sample = result.full_text[:1000]
            detected_lang = _detect_language_from_text(sample)
            if detected_lang and detected_lang != "und":
                return detected_lang
        except Exception:
            pass
    
    return "en"  # fallback, но это должно быть крайним случаем


def _work_to_source_result(work: dict, source_name: str = "CrossRef") -> SourceResult:
    """Преобразует один элемент message (или message.items[]) в SourceResult."""
    doi = (work.get("DOI") or "").strip()
    url = (work.get("URL") or "").strip() or (f"https://doi.org/{doi}" if doi else "")
    title_list = work.get("title") or []
    title = (title_list[0] if title_list else "").strip() or ""

    abstract_raw = work.get("abstract", "")
    if isinstance(abstract_raw, str):
        abstract = _strip_html_abstract(abstract_raw)
    else:
        abstract = ""

    authors = _parse_authors(work.get("author") or [])
    date = _parse_date(work)
    links = work.get("link") or []
    
    # Извлекаем все типы полных текстов с приоритетами
    full_text_info = _extract_full_text_links(links)
    pdf_url = full_text_info.get("pdf_url")

    meta: dict[str, Any] = {
        "container_title": (work.get("container-title") or [""])[0],
        "type": work.get("type", ""),
        "publisher": work.get("publisher", ""),
        # Сохраняем информацию о доступных полных текстах
        "full_text_links": {
            "pdf_url": pdf_url,
            "text_plain_url": full_text_info.get("text_plain_url"),
            "text_xml_url": full_text_info.get("text_xml_url"),
            "other_text_urls": full_text_info.get("other_text_urls", []),
        },
    }
    lang_from_api = _language_from_work(work)
    if lang_from_api:
        meta["language"] = lang_from_api

    return SourceResult(
        title=title,
        url=url,
        authors=authors,
        date=date,
        doi=doi or None,
        abstract=abstract,
        full_text="",
        pdf_url=pdf_url,
        metadata=meta,
        source_name=source_name,
    )


class CrossRefSource(BaseSource):
    """Поиск и загрузка метаданных через CrossRef REST API (без ключа)."""

    name = "crossref"

    async def __aenter__(self):
        """Контекстный менеджер: вход."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер: выход с закрытием сессии."""
        await self.close()

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._base = CROSSREF_API_BASE
        self._user_agent = DEFAULT_USER_AGENT
        self._timeout = 30
        self._request_delay = 1.0
        self._use_unpaywall = True
        self._unpaywall_email = ""
        self._unpaywall_delay = 0.3
        self._relevance_threshold = 0.2  # Порог релевантности по умолчанию (низкий для кириллицы/латиницы)
        try:
            import yaml
            root = Path(__file__).resolve().parent.parent.parent
            cfg_path = root / "config" / "settings.yaml"
            if cfg_path.exists():
                with open(cfg_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                crossref_cfg = (data.get("sources") or {}).get("crossref") or {}
                self._timeout = int(crossref_cfg.get("timeout", self._timeout))
                self._request_delay = float(crossref_cfg.get("request_delay", self._request_delay))
                if crossref_cfg.get("user_agent"):
                    self._user_agent = crossref_cfg["user_agent"]
                self._use_unpaywall = bool(crossref_cfg.get("use_unpaywall", True))
                self._unpaywall_email = (crossref_cfg.get("unpaywall_email") or "").strip()
                self._unpaywall_delay = float(crossref_cfg.get("unpaywall_delay", 0.3))
                self._relevance_threshold = float(crossref_cfg.get("relevance_threshold", self._relevance_threshold))
        except Exception:
            pass

    def _headers(self) -> dict:
        return {"User-Agent": self._user_agent}

    async def _get_session(self) -> aiohttp.ClientSession:
        """Создание сессии с блокировкой для thread-safety."""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                connector = aiohttp.TCPConnector(
                    limit=10,  # Ограничиваем количество соединений
                    limit_per_host=2,  # Ограничение на хост
                    ttl_dns_cache=300,
                    force_close=True,  # Принудительно закрываем соединения
                )
                self._session = aiohttp.ClientSession(
                    headers=self._headers(),
                    connector=connector,
                    timeout=aiohttp.ClientTimeout(total=self._timeout),
                )
            return self._session

    async def close(self):
        """Явное закрытие сессии."""
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None

    async def _request(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self._base}{path}"
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"
        session = await self._get_session()
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=self._timeout)) as resp:
                if resp.status == 429 or resp.status == 403:
                    raise SourceTemporarilyUnavailableError(
                        f"CrossRef API rate limit or blocked: {resp.status}",
                        source_name=self.name,
                    )
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientError as e:
            logger.warning("CrossRef request failed %s: %s", url, e)
            raise

    @staticmethod
    def _is_html_page(content: str) -> bool:
        """Проверяет, является ли контент HTML-страницей (а не чистым текстом/XML статьи)."""
        if not content:
            return False
        start = content.strip()[:1000].lower()
        return (
            "<!doctype html" in start
            or "<!doctype " in start
            or "<html" in start
            or ("<head>" in start and "<body>" in start)
            or "<meta charset" in start
            or "<meta http-equiv" in start
            or "<meta name=" in start
            or ("<link rel=" in start and "<script" in start)
        )

    @staticmethod
    def _strip_xml_to_text(content: str) -> Optional[str]:
        """Извлекает чистый текст из JATS/XML, отбрасывая теги, атрибуты, SVG, MathML и т.д."""
        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            return None

        # Теги, содержимое которых нужно пропустить целиком
        skip_tags = {
            "ref-list", "ref", "table-wrap", "fig", "graphic", "inline-graphic",
            "svg", "math", "mml:math", "xlink", "supplementary-material",
            "object-id", "contrib-id",
        }
        # Теги-секции, которые содержат полезный текст
        text_tags = {
            "article-title", "title", "p", "sec", "abstract", "body",
            "kwd", "kwd-group", "trans-abstract", "ack",
        }

        text_parts: list[str] = []

        def _local(tag: str) -> str:
            """Убирает namespace из тега: {http://...}body -> body."""
            if "}" in tag:
                return tag.split("}", 1)[1]
            return tag

        def _walk(elem):
            local = _local(elem.tag) if isinstance(elem.tag, str) else ""
            if local in skip_tags:
                return
            txt = (elem.text or "").strip()
            if txt and len(txt) > 3:
                text_parts.append(txt)
            for child in elem:
                _walk(child)
            tail = (elem.tail or "").strip()
            if tail and len(tail) > 3:
                text_parts.append(tail)

        _walk(root)

        if not text_parts:
            return None
        result = "\n\n".join(text_parts)
        # Если текст слишком короткий или всё ещё выглядит как HTML — отбрасываем
        if len(result) < 200:
            return None
        return result

    async def _fetch_text_content(
        self, url: str, content_type: str = "text/plain", timeout_sec: int = 30
    ) -> Optional[str]:
        """
        Загружает текстовый контент по URL (text/plain, text/xml и т.д.).
        Учитывает rate limits из заголовков CR-TDM-Rate-Limit.
        Отбрасывает HTML-страницы (редиректы на сайт издателя).
        """
        session = await self._get_session()
        try:
            headers = {
                "User-Agent": self._user_agent,
                "Accept": content_type,
            }
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_sec),
                allow_redirects=True,
            ) as resp:
                # Проверяем rate limits из заголовков
                rate_limit = resp.headers.get("CR-TDM-Rate-Limit")
                rate_remaining = resp.headers.get("CR-TDM-Rate-Limit-Remaining")
                rate_reset = resp.headers.get("CR-TDM-Rate-Limit-Reset")
                
                if rate_limit and rate_remaining:
                    logger.debug(
                        f"CrossRef TDM rate limit: {rate_remaining}/{rate_limit} (reset: {rate_reset})"
                    )
                    try:
                        if int(rate_remaining) <= 0:
                            logger.warning("CrossRef TDM rate limit exceeded, skipping text download")
                            return None
                    except ValueError:
                        pass
                
                if resp.status == 429:
                    logger.warning("CrossRef: rate limit hit for text content")
                    return None
                
                if resp.status != 200:
                    return None

                # Проверяем Content-Type ответа — сервер мог вернуть HTML вместо запрошенного типа
                resp_ct = (resp.headers.get("Content-Type") or "").lower()
                if "text/html" in resp_ct:
                    logger.debug("CrossRef: server returned text/html instead of %s for %s, skipping", content_type, url[:100])
                    return None

                content = await resp.text()
                
                # Защита: если контент — полноценная HTML-страница, ВСЕГДА отбрасываем
                # (даже если запрашивали XML — сервер мог сделать redirect на сайт издателя)
                if self._is_html_page(content):
                    logger.debug("CrossRef: fetched content is an HTML page (redirect?), discarding for %s", url[:100])
                    return None
                
                # Дополнительная проверка: если контент содержит типичные HTML-признаки
                content_lower_start = content.strip()[:2000].lower()
                if any(marker in content_lower_start for marker in [
                    "<meta ", "<link ", "<script", "<style", "<nav ", "<header", "<footer",
                    "<!doctype", "<html", "<head>", "<body>",
                    "class=\"", "id=\"", "href=\"/", "xmlns:",
                ]):
                    logger.debug("CrossRef: content contains HTML markers, discarding for %s", url[:100])
                    return None

                # Для XML парсим и извлекаем чистый текст (JATS XML и аналоги)
                if "xml" in content_type.lower() or "xml" in resp_ct:
                    extracted = self._strip_xml_to_text(content)
                    if extracted and len(extracted) > 200:
                        return extracted
                    logger.debug("CrossRef: XML text extraction yielded too little text for %s", url[:100])
                    return None
                
                # Для text/plain — дополнительная проверка, что это не HTML/XML
                if content.strip().startswith("<"):
                    logger.debug("CrossRef: text/plain content looks like markup, discarding for %s", url[:100])
                    return None

                return content
        except Exception as e:
            logger.debug(f"Failed to fetch text content from {url}: {e}")
            return None

    async def _fetch_unpaywall_pdf_url(self, doi: str) -> Optional[str]:
        """Запрос к Unpaywall API по DOI; возвращает url_for_pdf из best_oa_location или None."""
        if not self._use_unpaywall or not self._unpaywall_email or not doi:
            return None
        path = f"/v2/{quote(doi, safe='')}"
        url_req = f"{UNPAYWALL_API_BASE}{path}?email={quote(self._unpaywall_email, safe='')}"
        session = await self._get_session()
        try:
            async with session.get(
                url_req, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        except Exception as e:
            logger.debug("Unpaywall request for DOI %s failed: %s", doi, e)
            return None
        loc = data.get("best_oa_location") if isinstance(data, dict) else None
        if not isinstance(loc, dict):
            return None
        pdf_url = (loc.get("url_for_pdf") or loc.get("url") or "").strip()
        return pdf_url or None

    async def search(
        self,
        query: str,
        *,
        max_results: int = 50,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        languages: Optional[list[str]] = None,
    ) -> list[SourceResult]:
        """Поиск с улучшенной фильтрацией по релевантности."""
        if not (query or "").strip():
            print("[crossref] Empty query, returning empty results", flush=True)
            return []
        
        query_clean = query.strip()[:500]
        print(f"[crossref] Starting search: query='{query_clean[:100]}', max_results={max_results}, date_from={date_from}, date_to={date_to}", flush=True)
        results: list[SourceResult] = []
        
        # Сбрасываем счетчик отладки для нового запроса
        if hasattr(_calculate_relevance_score, '_debug_count'):
            _calculate_relevance_score._debug_count = 0
        
        # Поисковый запрос: используем query.bibliographic (поддерживается API)
        # query.title-abstract не поддерживается CrossRef API
        params: dict[str, Any] = {
            "query.bibliographic": query_clean,
            "rows": min(max_results * 2, 100),  # Ищем больше для последующей фильтрации
            # sort=relevance может не поддерживаться, убираем для совместимости
            # Вместо этого полагаемся на встроенную релевантность API и нашу фильтрацию
        }
        
        # Фильтры по дате публикации
        filters = []
        if date_from:
            filters.append(f"from-pub-date:{date_from}")
        if date_to:
            filters.append(f"until-pub-date:{date_to}")
        
        if filters:
            params["filter"] = ",".join(filters)

        print(f"[crossref] Requesting CrossRef API with params: {params}", flush=True)
        try:
            data = await self._request("/works", params=params)
            print(f"[crossref] Received response from CrossRef API", flush=True)
        except SourceTemporarilyUnavailableError as e:
            print(f"[crossref] SourceTemporarilyUnavailableError: {e}", flush=True)
            raise
        except aiohttp.ClientError as e:
            logger.error(f"CrossRef network error: {e}")
            print(f"[crossref] Network error: {e}", flush=True)
            return []
        except Exception as e:
            logger.exception("CrossRef search failed: %s", e)
            print(f"[crossref] Exception during search: {e}", flush=True)
            import traceback
            print(f"[crossref] Traceback: {traceback.format_exc()}", flush=True)
            return []

        msg = data.get("message") or {}
        items = msg.get("items") or []
        total_found = msg.get("total-results", 0)
        
        logger.info(
            f"CrossRef API: запрос '{query_clean}' вернул {len(items)} результатов "
            f"(всего найдено: {total_found})"
        )
        print(f"[crossref] CrossRef API returned {len(items)} items (total found: {total_found})", flush=True)
        
        for work in items:
            try:
                result = _work_to_source_result(work, source_name=self.name)
                # Сохраняем оригинальный query в metadata для расчета релевантности
                result.metadata["_original_query"] = query_clean
                results.append(result)
            except Exception as e:
                logger.debug("CrossRef skip item: %s", e)
                continue
        
        if not results:
            logger.warning(f"CrossRef: не удалось преобразовать ни одного результата из {len(items)} элементов")
            return []

        # Дополняем PDF-ссылками из Unpaywall, если CrossRef не вернул pdf_url
        if self._use_unpaywall and self._unpaywall_email:
            enriched: list[SourceResult] = []
            for r in results:
                if r.pdf_url or not r.doi:
                    enriched.append(r)
                    continue
                pdf_url = await self._fetch_unpaywall_pdf_url(r.doi)
                await asyncio.sleep(self._unpaywall_delay)
                if pdf_url:
                    # Обновляем metadata с новой PDF ссылкой
                    meta = dict(r.metadata or {})
                    full_text_links = meta.get("full_text_links", {})
                    full_text_links["pdf_url"] = pdf_url
                    meta["full_text_links"] = full_text_links
                    r = SourceResult(
                        title=r.title,
                        url=r.url,
                        authors=r.authors,
                        date=r.date,
                        doi=r.doi,
                        abstract=r.abstract,
                        full_text=r.full_text,
                        pdf_url=pdf_url,
                        metadata=meta,
                        source_name=r.source_name,
                    )
                enriched.append(r)
            results = enriched
        
        # Загружаем текстовые форматы полных текстов (text/plain, text/xml) для документов без PDF
        for r in results:
            full_text_links = (r.metadata or {}).get("full_text_links", {})
            # Если есть PDF, пропускаем текстовые форматы
            if r.pdf_url:
                continue
            
            # Пробуем загрузить text/plain
            text_plain_url = full_text_links.get("text_plain_url")
            if text_plain_url:
                try:
                    text_content = await self._fetch_text_content(text_plain_url, "text/plain", timeout_sec=25)
                    if text_content and len(text_content) > 500:
                        r.full_text = text_content
                        await asyncio.sleep(self._request_delay)  # Задержка между запросами
                        continue  # Нашли text/plain, пропускаем text/xml
                except Exception:
                    pass
            
            # Если text/plain не удалось, пробуем text/xml
            text_xml_url = full_text_links.get("text_xml_url")
            if text_xml_url:
                try:
                    text_content = await self._fetch_text_content(text_xml_url, "text/xml", timeout_sec=25)
                    if text_content and len(text_content) > 500:
                        r.full_text = text_content
                        await asyncio.sleep(self._request_delay)
                except Exception:
                    pass

        # Фильтрация по релевантности: проверяем ключевые слова в заголовке/аннотации
        query_lower = query_clean.lower()
        query_words = [w.strip() for w in query_lower.split() if len(w.strip()) > 2]
        
        filtered_results = []
        relevance_scores = []
        for r in results:
            # Рассчитываем релевантность
            relevance_score = _calculate_relevance_score(r, query_clean)
            # Сохраняем score в metadata для использования в to_rag_document
            r.metadata["_relevance_score"] = relevance_score
            relevance_scores.append(relevance_score)
            
            # Отбираем только достаточно релевантные документы
            if relevance_score >= self._relevance_threshold:
                filtered_results.append(r)
            else:
                # Детальное логирование для первых 3 отфильтрованных документов
                if len([s for s in relevance_scores if s < self._relevance_threshold]) <= 3:
                    logger.info(
                        f"CrossRef: Document filtered by relevance (score={relevance_score:.3f} < threshold={self._relevance_threshold}): "
                        f"title='{r.title[:80]}', abstract='{r.abstract[:100] if r.abstract else 'N/A'}...'"
                    )
                else:
                    logger.debug(
                        f"CrossRef: Document filtered by relevance (score={relevance_score:.2f} < threshold={self._relevance_threshold}): "
                        f"'{r.title[:60]}...'"
                    )
        
        # Логирование статистики релевантности
        if results:
            avg_relevance = sum(relevance_scores) / len(relevance_scores)
            max_relevance = max(relevance_scores) if relevance_scores else 0.0
            logger.info(
                f"CrossRef: relevance filtering - found {len(results)} documents, "
                f"avg_score={avg_relevance:.2f}, max_score={max_relevance:.2f}, "
                f"threshold={self._relevance_threshold}, passed={len(filtered_results)}"
            )
        
        # Fallback: если ни один документ не прошёл порог — берём топ max_results по релевантности
        if not filtered_results and results:
            sorted_by_relevance = sorted(
                zip(results, relevance_scores), key=lambda x: x[1], reverse=True
            )
            filtered_results = [r for r, _ in sorted_by_relevance[:max_results]]
            logger.info(
                f"CrossRef: no documents passed threshold; using top {len(filtered_results)} by relevance score"
            )
        
        results = filtered_results[:max_results]  # Обрезаем до нужного количества
        
        # Фильтрация по языку с подробным логированием
        if languages:
            allowed = {_normalize_lang_code(l) for l in languages if l and isinstance(l, str)}
            if allowed:
                filtered_by_lang = []
                for r in results:
                    lang = _get_result_language(r)
                    # Проверяем, что язык определен и входит в разрешенные
                    if lang and lang in allowed:
                        filtered_by_lang.append(r)
                    else:
                        logger.debug(
                            f"CrossRef: Document filtered by language: '{r.title[:50]}...' -> "
                            f"detected: {lang}, allowed: {allowed}"
                        )
                
                if len(filtered_by_lang) != len(results):
                    logger.info(
                        f"CrossRef: Language filter: {len(results)} -> {len(filtered_by_lang)} documents "
                        f"(allowed languages: {allowed})"
                    )
                
                results = filtered_by_lang
        
        # Логирование релевантности
        if results:
            avg_relevance = sum(r.metadata.get("_relevance_score", 0.0) for r in results) / len(results)
            logger.info(
                f"CrossRef: запрос '{query_clean}', найдено {len(items)} результатов из API, "
                f"после фильтрации по релевантности (threshold={self._relevance_threshold}): {len(filtered_results)}, "
                f"после фильтра по языку: {len(results)}, "
                f"средняя релевантность: {avg_relevance:.2f}"
            )
        else:
            # Детальное логирование если нет результатов
            if len(items) > 0:
                logger.warning(
                    f"CrossRef: запрос '{query_clean}' вернул {len(items)} результатов из API, "
                    f"но все были отфильтрованы (relevance_threshold={self._relevance_threshold})"
                )
                # Показываем примеры отфильтрованных документов для отладки
                if filtered_results:
                    logger.debug(f"Примеры отфильтрованных документов (первые 3):")
                    for r in filtered_results[:3]:
                        score = r.metadata.get("_relevance_score", 0.0)
                        logger.debug(f"  - '{r.title[:60]}...' (score={score:.2f})")
            else:
                logger.warning(f"CrossRef: запрос '{query_clean}' не вернул результатов из API")

        print(f"[crossref] Final results: {len(results)} documents", flush=True)
        return results

    async def fetch_article(self, url: str) -> Optional[SourceResult]:
        """Загрузка метаданных работы по URL (https://doi.org/10.xxx/yyy) или по DOI."""
        doi = url.strip()
        if doi.startswith("https://doi.org/"):
            doi = doi.replace("https://doi.org/", "", 1)
        elif doi.startswith("http://doi.org/"):
            doi = doi.replace("http://doi.org/", "", 1)
        doi = doi.split("?")[0].rstrip("/")
        if not doi or "/" not in doi:
            return None
        path = f"/works/{quote(doi, safe='')}"
        try:
            data = await self._request(path)
        except SourceTemporarilyUnavailableError:
            raise
        except Exception as e:
            logger.warning("CrossRef fetch_article %s: %s", url, e)
            return None
        msg = data.get("message")
        if not msg:
            return None
        return _work_to_source_result(msg, source_name=self.name)

    def to_rag_document(
        self,
        result: SourceResult,
        *,
        chunk_size_min: int = 500,
        chunk_size_max: int = 2000,
        overlap: int = 100,
    ) -> RAGDocument:
        """Преобразование SourceResult в RAGDocument (как у CyberLeninka): чанкование, язык, валидация."""
        raw_id = f"{result.url}{result.title}"
        doc_id = hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:32]

        # Загружаем полные тексты с приоритетами: PDF > text/plain > text/xml
        text_from_pdf: Optional[str] = None
        text_from_plain: Optional[str] = None
        text_from_xml: Optional[str] = None
        
        full_text_links = (result.metadata or {}).get("full_text_links", {})
        
        # 1. Пробуем загрузить PDF
        if result.pdf_url:
            try:
                text_from_pdf = extract_text_from_pdf_url(
                    result.pdf_url, timeout_sec=25, max_bytes=15 * 1024 * 1024
                )
            except Exception:
                text_from_pdf = None
        
        
        # Выбираем лучший доступный текст для чанкования
        # Приоритет: PDF > загруженный full_text (text/plain или text/xml) > abstract
        html_text = (result.full_text or "").strip()
        # Защита: если full_text выглядит как HTML-страница — отбрасываем
        if html_text and self._is_html_page(html_text):
            logger.debug("CrossRef to_rag_document: full_text is HTML page, discarding for '%s'", (result.title or "")[:60])
            html_text = ""
        # Защита: если full_text содержит HTML-признаки (теги, атрибуты, SVG и т.д.)
        if html_text:
            text_lower_start = html_text[:2000].lower()
            html_markers = [
                "<meta ", "<link ", "<script", "<style", "<nav ", "<header", "<footer",
                "<!doctype", "<html", "<head>", "<body>",
                "class=\"", "xmlns:", "<svg", "<path d=",
            ]
            if any(marker in text_lower_start for marker in html_markers):
                logger.info("CrossRef to_rag_document: full_text contains HTML/SVG markers, discarding for '%s'", (result.title or "")[:60])
                html_text = ""
        # Защита: если full_text начинается с тегов и мало чистого текста — отбрасываем
        if html_text and html_text.lstrip().startswith("<"):
            clean_text = re.sub(r"<[^>]+>", "", html_text).strip()
            if len(clean_text) < 200:
                logger.debug("CrossRef to_rag_document: full_text looks like markup with little text, discarding for '%s'", (result.title or "")[:60])
                html_text = ""
        if text_from_pdf and len(text_from_pdf) > 500:
            text_to_chunk = text_from_pdf
        elif html_text and len(html_text) > 500:
            # Используем уже загруженный full_text из search()
            text_to_chunk = html_text
        elif text_from_pdf and len(text_from_pdf) > 200 and len(text_from_pdf) >= len(html_text):
            text_to_chunk = text_from_pdf
        else:
            text_to_chunk = result.abstract

        if text_to_chunk and text_to_chunk.strip():
            _, text_to_chunk = validate_text_ends_complete(text_to_chunk)

        chunk_size_min = int(_text_processing_config("chunk_size_tokens_min", 500) or 500)
        chunk_size_max = int(_text_processing_config("chunk_size_tokens_max", 2000) or 2000)
        overlap = int(_text_processing_config("chunk_overlap_tokens", 100) or 100)
        max_chunks_val = _text_processing_config("max_chunks_per_document", 25)
        max_chunks = int(max_chunks_val) if max_chunks_val is not None else 25
        use_emb = _text_processing_config("use_embedding_chunker", False)
        sim_thr = _text_processing_config("similarity_threshold", 0.5)
        emb_model = _text_processing_config(
            "embedding_model", "sentence-transformers/all-MiniLM-L6-v2"
        ) or "sentence-transformers/all-MiniLM-L6-v2"
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
                similarity_threshold=float(sim_thr) if sim_thr is not None else 0.5,
                embedding_model=emb_model,
            ) if text_to_chunk else []
        except MemoryError:
            fallback = (result.abstract or "")[:8000] if result.abstract else (text_to_chunk or "")[:8000]
            chunks = [fallback] if fallback.strip() else []

        chunks = ensure_chunks_end_at_boundaries(chunks)
        chunks = dedupe_chunks_by_hash(chunks)
        chunks = filter_and_clean_chunks(chunks)

        chunk_strategy = "semantic_embedding" if use_emb else "semantic_paragraph"
        chunk_info = compute_chunk_info(chunks, strategy=chunk_strategy)
        
        # Определение языка: если есть полный текст, используем его для более точного определения
        if text_to_chunk and len(text_to_chunk) > 500:
            language = _detect_language_from_text(text_to_chunk)
        else:
            language = _get_result_language(result)

        # Статус доступности полного текста для RAG
        full_text_links = (result.metadata or {}).get("full_text_links", {})
        if text_from_pdf and len(text_from_pdf) > 500:
            file_access = "pdf_direct_link"
        elif html_text and len(html_text) > 500:
            # Определяем тип загруженного текста
            if full_text_links.get("text_plain_url"):
                file_access = "text_plain_link"
            elif full_text_links.get("text_xml_url"):
                file_access = "text_xml_link"
            else:
                file_access = "html_full_text"
        elif text_to_chunk and (text_to_chunk != (result.abstract or "").strip() and len(text_to_chunk or "") > len(result.abstract or "")):
            file_access = "html_full_text"
        else:
            file_access = "abstract_only"

        metadata = dict(result.metadata or {})
        if text_from_pdf:
            pdf_meta = extract_metadata_from_pdf_text(text_from_pdf)
            metadata.update(pdf_meta)
        title_abstract = (result.title or "") + " " + (result.abstract or "")
        body_preview = (text_to_chunk or "")[:1000] if text_to_chunk else ""
        metadata["document_type"] = document_type_from_text(
            result.title or "", result.abstract or "", body_preview
        )
        metadata["is_dissertation"] = is_dissertation(title_abstract)

        files: list[FileRef] = []
        if result.pdf_url:
            files.append(FileRef(type="PDF", url=result.pdf_url, extracted_text=text_from_pdf))

        # Получаем relevance_score из metadata (если был рассчитан в search) или рассчитываем заново
        relevance_score = metadata.get("_relevance_score")
        if relevance_score is None:
            # Если не был рассчитан, пытаемся рассчитать по оригинальному query
            original_query = metadata.get("_original_query")
            if original_query:
                relevance_score = _calculate_relevance_score(result, original_query)
            else:
                relevance_score = None
        
        # Удаляем служебные поля из metadata перед сохранением
        metadata_clean = {k: v for k, v in metadata.items() if not k.startswith("_")}

        doc = RAGDocument(
            id=doc_id,
            title=result.title or "Без названия",
            authors=result.authors,
            date=normalize_date(result.date) if result.date else result.date,
            doi=result.doi,
            url=result.url,
            language=language,
            source="CrossRef",
            abstract=result.abstract or "",
            full_text_chunks=chunks,
            files=files,
            metadata=metadata_clean,
            processing_info=ProcessingInfo(
                extraction_method="api",
                chunking_strategy=chunk_strategy,
                validation_score=0.0,
                chunk_info=chunk_info,
            ),
            relevance_score=relevance_score,
            file_access=file_access,
            crawling_timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        doc.processing_info.validation_score = compute_validation_score(doc)
        return doc


register_source("crossref", CrossRefSource)
