"""Тесты источника CyberLeninka (с моком HTML)."""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.sources.cyberleninka import CyberLeninkaSource
from src.sources.base import SourceResult


SEARCH_HTML = """
<html>
<body>
<article class="search-result-item">
  <a href="/article/n/12345">Статья о климате</a>
</article>
<article>
  <a href="/article/n/67890">Вторая статья</a>
</article>
</body>
</html>
"""

ARTICLE_HTML = """
<html>
<body>
<h1 class="article__title">Тестовая статья о климате</h1>
<div class="author">Иванов И.И.</div>
<div class="abstract">Аннотация статьи о климатических изменениях в регионах.</div>
<div class="article__body">Полный текст статьи. Много предложений для чанкования. </div>
<a href="/article/n/12345/pdf">Скачать PDF</a>
</body>
</html>
"""


@pytest.mark.asyncio
async def test_parse_search_page():
    source = CyberLeninkaSource(base_url="https://cyberleninka.ru", request_delay=0)
    links = source._parse_search_page(SEARCH_HTML, "https://cyberleninka.ru")
    assert len(links) >= 1
    assert any("/article/" in url for _, url in links)


@pytest.mark.asyncio
async def test_parse_article_page():
    source = CyberLeninkaSource()
    result = source._parse_article_page(
        ARTICLE_HTML,
        "https://cyberleninka.ru/article/n/12345",
    )
    assert result is not None
    assert "климат" in result.title or "Тестовая" in result.title
    assert result.abstract
    assert result.source_name == "cyberleninka"


def test_to_rag_document():
    source = CyberLeninkaSource()
    raw = SourceResult(
        title="Тест",
        url="https://cyberleninka.ru/article/n/1",
        authors=["Автор"],
        abstract="Аннотация текста.",
        full_text="Полный текст " * 100,
        source_name="cyberleninka",
    )
    doc = source.to_rag_document(raw, chunk_size_min=50, chunk_size_max=200, overlap=20)
    assert doc.title == "Тест"
    assert doc.source == "CyberLeninka"
    assert doc.language
    assert len(doc.full_text_chunks) >= 1
    assert doc.processing_info.validation_score >= 0
