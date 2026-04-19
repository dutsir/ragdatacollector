"""Помощник для формулировки эффективных запросов."""
from typing import Any


class QueryAssistant:
    """Помощник для формулировки эффективных запросов."""

    def suggest_improvements(self, original_query: dict[str, Any]) -> dict[str, Any]:
        """Предлагает улучшения для запроса на основе лучших практик."""
        suggestions: dict[str, Any] = {
            "keywords": [],
            "filters": [],
            "sources": [],
        }
        keywords = original_query.get("keywords", [])

        if len(keywords) == 1 and " " not in (keywords[0] or ""):
            suggestions["keywords"].append(
                "Добавьте больше ключевых слов для точности поиска"
            )
        if any(kw and len(kw) < 3 for kw in keywords):
            suggestions["keywords"].append(
                "Используйте более конкретные ключевые слова (минимум 3 символа)"
            )

        languages = original_query.get("languages", [])
        if not languages:
            suggestions["filters"].append(
                "Укажите языки для поиска, иначе будут найдены все языки"
            )

        date_range = original_query.get("date_range") or (
            original_query.get("date_from") and original_query.get("date_to")
        )
        if not date_range and not original_query.get("date_from"):
            suggestions["filters"].append(
                "Укажите временной диапазон для актуальности результатов"
            )

        sources = original_query.get("sources", [])
        if not sources:
            suggestions["sources"] = self._suggest_sources_by_topic(keywords)

        return suggestions

    def _suggest_sources_by_topic(self, keywords: list[str]) -> list[str]:
        """Предлагает источники на основе тематики."""
        topic_to_sources: dict[str, list[str]] = {
            "научный": ["cyberleninka", "arxiv", "openalex", "crossref"],
            "медицинский": ["pubmed", "google_serp"],
            "технический": ["arxiv", "google_serp"],
            "гуманитарный": ["cyberleninka", "google_serp"],
            "новостной": ["google_serp"],
            "юридический": ["cyberleninka", "google_serp"],
            "экономический": ["cyberleninka", "google_serp"],
            "исторический": ["cyberleninka", "google_serp"],
        }
        all_keywords = " ".join(k or "" for k in keywords).lower()
        suggested: set[str] = {"cyberleninka", "google_serp"}
        for topic, sources in topic_to_sources.items():
            if topic in all_keywords:
                suggested.update(sources)
        return list(suggested)
