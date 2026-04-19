"""Опциональное получение DOI по заголовку/автору через Crossref API (без ключа)."""
from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Optional


def fetch_doi_from_crossref(
    title: str,
    authors: Optional[list[str]] = None,
    timeout_sec: int = 5,
) -> Optional[str]:
    """
    Ищет DOI по заголовку (и опционально авторам) через Crossref REST API.
    Возвращает первый найденный DOI или None. API не требует ключа надо потпм посмотреь что можно будет изменит.
    """
    if not title or not title.strip():
        return None
    query = title.strip()
    if authors and authors:
        query = f"{query} {' '.join(authors[:3])}"
    query = query[:200]
    url = "https://api.crossref.org/works?" + urllib.parse.urlencode(
        {"query.bibliographic": query, "rows": 1},
        encoding="utf-8",
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "RAG-Collector/1.0 (mailto:dev@example.org)"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None
    items = (data.get("message") or {}).get("items") or []
    if not items:
        return None
    doi = items[0].get("DOI")
    if doi and isinstance(doi, str):
        return doi.strip().rstrip("/")
    return None
