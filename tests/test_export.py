"""Тесты экспорта NDJSON."""
import tempfile
from pathlib import Path

import pytest
from src.models.document import RAGDocument, ProcessingInfo
from src.export_ndjson import export_to_ndjson, load_ndjson


def test_export_and_load_ndjson():
    docs = [
        RAGDocument(
            id="id1",
            title="Title 1",
            url="https://example.com/1",
            language="ru",
            source="Test",
            abstract="Abstract 1",
            full_text_chunks=["chunk1"],
            processing_info=ProcessingInfo(validation_score=0.9),
        ),
    ]
    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
        path = f.name
    try:
        export_to_ndjson(docs, path, include_processing_info=True)
        loaded = load_ndjson(path)
        assert len(loaded) == 1
        assert loaded[0]["title"] == "Title 1"
        assert loaded[0]["id"] == "id1"
        assert "processing_info" in loaded[0]
    finally:
        Path(path).unlink(missing_ok=True)


def test_export_without_processing_info():
    docs = [
        RAGDocument(
            id="id2",
            title="Title 2",
            url="https://example.com/2",
            language="en",
            source="Test",
        ),
    ]
    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
        path = f.name
    try:
        export_to_ndjson(docs, path, include_processing_info=False)
        loaded = load_ndjson(path)
        assert len(loaded) == 1
        assert "processing_info" not in loaded[0] or loaded[0].get("processing_info") is None
    finally:
        Path(path).unlink(missing_ok=True)
