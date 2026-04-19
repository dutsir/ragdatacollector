"""Тесты UniversalQueryOptimizer."""
import pytest
from src.query.optimizer import UniversalQueryOptimizer


def test_optimize_simple_cyberleninka():
    opt = UniversalQueryOptimizer()
    q = opt.optimize_query(
        source="cyberleninka",
        keywords=["история России"],
        languages=["ru"],
    )
    assert "история" in q or "России" in q


def test_optimize_with_date_range():
    opt = UniversalQueryOptimizer()
    q = opt.optimize_query(
        source="cyberleninka",
        keywords=["климат"],
        date_range={"from": "2020-01-01", "to": "2024-12-31"},
        languages=["ru"],
    )
    assert "2020" in q or "2024" in q or "климат" in q


def test_generate_search_url_cyberleninka():
    opt = UniversalQueryOptimizer()
    url = opt.generate_search_url(
        source="cyberleninka",
        query="тест",
        max_results=20,
    )
    assert "cyberleninka.ru" in url
    assert "q=" in url or "query" in url.lower()


def test_auto_detect_query_type():
    opt = UniversalQueryOptimizer()
    analysis = opt.auto_detect_query_type(["метод", "анализ данных"])
    assert "contains_phrases" in analysis
    assert "contains_academic_terms" in analysis
