"""Универсальный оптимизатор поисковых запросов для разных источников."""
from __future__ import annotations

from datetime import datetime
from typing import Any

import urllib.parse


class UniversalQueryOptimizer:
    """Универсальный оптимизатор поисковых запросов для разных источников."""

    SOURCE_OPERATORS = {
        "cyberleninka": {
            "exact_phrase": '"{phrase}"',
            "required_word": "+{word}",
            "exclude_word": "-{word}",
            "language": "язык:{lang}",
            "year_range": "year:{start}-{end}",
            "section": "section:{section}",
        },
        "google": {
            "exact_phrase": '"{phrase}"',
            "required_word": "+{word}",
            "exclude_word": "-{word}",
            "site": "site:{site}",
            "filetype": "filetype:{ext}",
            "daterange": "daterange:{start}-{end}",
            "after": "after:{date}",
            "before": "before:{date}",
            "intitle": "intitle:{word}",
            "inurl": "inurl:{word}",
        },
        "google_serp": {
            "exact_phrase": '"{phrase}"',
            "required_word": "+{word}",
            "exclude_word": "-{word}",
            "site": "site:{site}",
            "filetype": "filetype:{ext}",
            "after": "after:{date}",
            "before": "before:{date}",
        },
        "arxiv": {
            "all_fields": "all:{query}",
            "title": "ti:{query}",
            "author": "au:{query}",
            "abstract": "abs:{query}",
            "category": "cat:{category}",
            "date": "submittedDate:[{start} TO {end}]",
        },
    }

    LANGUAGE_MAP = {
        "ru": {"cyberleninka": "русский", "google": "lang_ru", "google_serp": "lang_ru"},
        "en": {"cyberleninka": "английский", "google": "lang_en", "google_serp": "lang_en"},
        "zh": {"cyberleninka": "китайский", "google": "lang_zh-CN", "google_serp": "lang_zh-CN"},
        "de": {"cyberleninka": "немецкий", "google": "lang_de", "google_serp": "lang_de"},
        "fr": {"cyberleninka": "французский", "google": "lang_fr", "google_serp": "lang_fr"},
        "es": {"cyberleninka": "испанский", "google": "lang_es", "google_serp": "lang_es"},
        "ja": {"cyberleninka": "японский", "google": "lang_ja", "google_serp": "lang_ja"},
        "ko": {"cyberleninka": "корейский", "google": "lang_ko", "google_serp": "lang_ko"},
        "ar": {"cyberleninka": "арабский", "google": "lang_ar", "google_serp": "lang_ar"},
        "he": {"cyberleninka": "иврит", "google": "lang_iw", "google_serp": "lang_iw"},
        "yi": {"cyberleninka": "идиш", "google": "lang_yi", "google_serp": "lang_yi"},
    }

    def __init__(self) -> None:
        self.query_templates = {
            "simple": "{keywords}",
            "advanced": "({sections}) AND ({keywords}) AND ({filters})",
            "academic": "({field}:{query}) AND ({filters})",
        }

    def optimize_query(
        self,
        source: str,
        keywords: list[str],
        date_range: dict[str, str] | None = None,
        languages: list[str] | None = None,
        file_types: list[str] | None = None,
        domains: list[str] | None = None,
        sections: list[str] | None = None,
        exact_match: bool = False,
    ) -> str:
        """Универсальная оптимизация запроса для любого источника."""
        if source not in self.SOURCE_OPERATORS:
            return " ".join(kw for kw in keywords if kw and kw.strip())

        operators = self.SOURCE_OPERATORS[source]
        query_parts: list[str] = []

        keyword_query = self._process_keywords(keywords, operators, exact_match, source)
        if keyword_query:
            query_parts.append(keyword_query)

        # Для CyberLeninka фильтры языка и даты применяются через UI, не добавляем их в текстовый запрос
        if languages and source != "cyberleninka":
            language_filters = self._build_language_filters(source, languages, operators)
            if language_filters:
                query_parts.append(language_filters)

        # Для arxiv даты передаются отдельно через параметры date_from/date_to, не добавляем их в запрос
        if date_range and source not in ("cyberleninka", "arxiv"):
            date_filter = self._build_date_filter(source, date_range, operators)
            if date_filter:
                query_parts.append(date_filter)

        if file_types and source in ("google", "google_serp"):
            filetype_filter = self._build_filetype_filter(file_types, operators)
            if filetype_filter:
                query_parts.append(filetype_filter)

        if domains and source in ("google", "google_serp"):
            site_filter = self._build_site_filter(domains, operators)
            if site_filter:
                query_parts.append(site_filter)

        if sections and source == "cyberleninka":
            section_filter = self._build_section_filter(sections, operators)
            if section_filter:
                query_parts.append(section_filter)

        return self._build_final_query(source, query_parts)

    def _process_keywords(
        self, keywords: list[str], operators: dict[str, str], exact_match: bool, source: str
    ) -> str:
        processed: list[str] = []
        for keyword in keywords:
            keyword = keyword.strip().rstrip(",").strip()  # Очищаем запятые и пробелы
            if not keyword:
                continue
            # Для CyberLeninka, arXiv и DuckDuckGo SERP не добавляем кавычки и операторы
            # arXiv: _build_search_query() сам формирует all:term1+AND+all:term2
            # google_serp: DuckDuckGo HTML лучше работает с простыми запросами
            if source in ("cyberleninka", "arxiv", "google_serp"):
                processed.append(keyword)
            elif " " in keyword or exact_match:
                if '"' not in keyword and "'" not in keyword:
                    processed.append(
                        operators.get("exact_phrase", '"{phrase}"').format(phrase=keyword)
                    )
                else:
                    processed.append(keyword)
            else:
                processed.append(
                    operators.get("required_word", "+{word}").format(word=keyword)
                )
        return " ".join(processed) if processed else ""

    def _build_language_filters(
        self, source: str, languages: list[str], operators: dict[str, str]
    ) -> str:
        filters: list[str] = []
        for lang in languages:
            if lang in self.LANGUAGE_MAP and source in self.LANGUAGE_MAP[lang]:
                lang_value = self.LANGUAGE_MAP[lang][source]
                if "language" in operators:
                    filters.append(operators["language"].format(lang=lang_value))
        if source == "cyberleninka" and filters:
            return " AND ".join(filters)
        return ""

    def _build_date_filter(
        self, source: str, date_range: dict[str, str], operators: dict[str, str]
    ) -> str:
        from_date = date_range.get("from")
        to_date = date_range.get("to")
        if not from_date or not to_date:
            return ""
        if source == "cyberleninka":
            try:
                start_year = datetime.strptime(from_date, "%Y-%m-%d").year
                end_year = datetime.strptime(to_date, "%Y-%m-%d").year
                return operators.get("year_range", "year:{start}-{end}").format(
                    start=start_year, end=end_year
                )
            except ValueError:
                return ""
        if source in ("google", "google_serp"):
            return (
                f"{operators.get('after', 'after:{date}').format(date=from_date)} "
                f"{operators.get('before', 'before:{date}').format(date=to_date)}"
            )
        if source == "arxiv":
            # arXiv требует формат YYYYMMDD (8 цифр без дефисов)
            try:
                from_dt = datetime.strptime(from_date, "%Y-%m-%d")
                to_dt = datetime.strptime(to_date, "%Y-%m-%d")
                start_str = from_dt.strftime("%Y%m%d")
                end_str = to_dt.strftime("%Y%m%d")
                return operators.get("date", "submittedDate:[{start} TO {end}]").format(
                    start=start_str, end=end_str
                )
            except ValueError:
                return ""
        return ""

    def _build_filetype_filter(
        self, file_types: list[str], operators: dict[str, str]
    ) -> str:
        if "filetype" not in operators:
            return ""
        return " OR ".join(
            operators["filetype"].format(ext=ft) for ft in file_types
        )

    def _build_site_filter(
        self, domains: list[str], operators: dict[str, str]
    ) -> str:
        if "site" not in operators:
            return ""
        return " OR ".join(operators["site"].format(site=d) for d in domains)

    def _build_section_filter(
        self, sections: list[str], operators: dict[str, str]
    ) -> str:
        if "section" not in operators:
            return ""
        return " OR ".join(
            operators["section"].format(section=s) for s in sections
        )

    def _build_final_query(self, source: str, query_parts: list[str]) -> str:
        valid_parts = [p for p in query_parts if p]
        if not valid_parts:
            return ""
        if source == "cyberleninka":
            return " AND ".join(valid_parts)
        if source in ("google", "google_serp", "arxiv"):
            return " ".join(valid_parts)
        return " ".join(valid_parts)

    def generate_search_url(
        self,
        source: str,
        query: str,
        languages: list[str] | None = None,
        max_results: int = 10,
    ) -> str:
        """Генерация полного URL для поиска."""
        base_urls = {
            "cyberleninka": "https://cyberleninka.ru/search",
            "google": "https://www.google.com/search",
            "google_serp": "https://www.google.com/search",
            "arxiv": "https://arxiv.org/search/advanced",
        }
        if source not in base_urls:
            raise ValueError(f"Unknown source: {source}")

        if source == "cyberleninka":
            params: dict[str, Any] = {"q": query, "page": 1}
            if languages and "ru" in languages:
                params["t"] = "article"
            return f"{base_urls[source]}?{urllib.parse.urlencode(params)}"

        if source in ("google", "google_serp"):
            params = {
                "q": query,
                "num": min(max_results, 100),
                "hl": "en",
                "lr": "",
            }
            if languages:
                lang_codes = []
                for lang in languages:
                    if lang in self.LANGUAGE_MAP and "google" in self.LANGUAGE_MAP[lang]:
                        lang_codes.append(self.LANGUAGE_MAP[lang]["google"])
                if lang_codes:
                    params["lr"] = "|".join(lang_codes)
            return f"{base_urls[source]}?{urllib.parse.urlencode(params)}"

        if source == "arxiv":
            params = {
                "searchtype": "all",
                "query": query,
                "abstracts": "show",
                "order": "-announced_date_first",
                "size": max_results,
            }
            return f"{base_urls[source]}?{urllib.parse.urlencode(params)}"

        return f"{base_urls[source]}?q={urllib.parse.quote(query)}"

    def auto_detect_query_type(self, keywords: list[str]) -> dict[str, Any]:
        """Автоматическое определение типа запроса."""
        analysis: dict[str, Any] = {
            "contains_phrases": False,
            "contains_academic_terms": False,
            "contains_names": False,
            "contains_technical_terms": False,
            "language_mix": [],
        }
        import re

        academic = [
            "метод", "анализ", "исследование", "method", "analysis", "study",
            "нейронн", "deep learning", "transformer",
        ]
        for keyword in keywords:
            if " " in keyword:
                analysis["contains_phrases"] = True
            if any(ind in keyword.lower() for ind in academic):
                analysis["contains_academic_terms"] = True
            if re.match(r"^[A-ZА-Я][a-zа-я]+ [A-ZА-Я]\. [A-ZА-Я]\.$", keyword):
                analysis["contains_names"] = True
            try:
                from langdetect import detect, LangDetectException

                lang = detect(keyword)
                if lang not in analysis["language_mix"]:
                    analysis["language_mix"].append(lang)
            except Exception:
                pass
        return analysis
