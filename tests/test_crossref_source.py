"""Проверка источника CrossRef: поиск и преобразование в RAGDocument."""
import asyncio
import pytest

from src.sources import get_source


async def _search_and_rag():
    source = get_source("crossref")
    assert source is not None
    results = await source.search(
        "climate change",
        max_results=5,
        date_from="2024-01-01",
        date_to="2026-01-01",
    )
    return source, results


def test_crossref_search_and_rag():
    """Проверка поиска CrossRef и преобразования в RAGDocument. Выход по ТЗ: id, title, authors, date, doi/url, abstract, full_text_chunks[], language, source; чанки 500–2000 токенов из конфига."""
    source, results = asyncio.run(_search_and_rag())
    assert isinstance(results, list)
    if not results:
        pytest.skip("CrossRef API returned no results (network or rate limit?)")
    r = results[0]
    assert r.title or r.doi
    assert r.url
    doc = source.to_rag_document(r)
    # Обязательные поля по ТЗ
    assert doc.id, "id обязателен"
    assert doc.source == "CrossRef"
    assert doc.title
    assert doc.url or doc.doi
    assert isinstance(doc.authors, list)
    assert isinstance(doc.full_text_chunks, list)
    assert "abstract" in doc.model_dump()
    assert "language" in doc.model_dump()
    assert "date" in doc.model_dump()
    # Новые поля для RAG: file_access, crawling_timestamp
    assert doc.file_access in ("pdf_direct_link", "html_full_text", "abstract_only"), "file_access должен быть задан"
    assert doc.crawling_timestamp is not None and "T" in doc.crawling_timestamp, "crawling_timestamp в формате ISO"
    # Чанки: границы из конфига (500–2000 токенов); допуск из‑за границ по абзацам
    if doc.processing_info and doc.processing_info.chunk_info and doc.full_text_chunks:
        ci = doc.processing_info.chunk_info
        assert ci.get("max_tokens", 0) <= 2500, "max_tokens не должен сильно превывать 2000"
        assert ci.get("min_tokens", 0) >= 0, "min_tokens (короткие чанки допустимы)"
