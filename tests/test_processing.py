"""Тесты обработки текста: чанкование, язык, валидация."""
import pytest
from src.processing.chunking import chunk_text, count_tokens
from src.processing.language import detect_language, is_rtl, is_cjk
from src.processing.validation import compute_validation_score, validate_document, content_hash
from src.models.document import RAGDocument, ProcessingInfo


def test_chunk_text_short():
    text = "Короткий текст."
    chunks = chunk_text(text, chunk_size_min=500, chunk_size_max=2000)
    assert len(chunks) >= 1
    assert chunks[0] == text or text in chunks[0]


def test_chunk_text_long():
    text = "Предложение одно. " * 500
    chunks = chunk_text(text, chunk_size_min=50, chunk_size_max=200, overlap_tokens=10)
    assert len(chunks) >= 2
    total_chars = sum(len(c) for c in chunks)
    assert total_chars >= len(text) * 0.5


def test_detect_language_ru():
    lang = detect_language("Это русский текст для проверки определения языка.")
    assert lang == "ru"


def test_detect_language_en():
    lang = detect_language("This is English text for language detection.")
    assert lang == "en"


def test_is_rtl():
    assert is_rtl("ar") is True
    assert is_rtl("he") is True
    assert is_rtl("en") is False


def test_is_cjk():
    assert is_cjk("zh") is True
    assert is_cjk("ja") is True
    assert is_cjk("en") is False


def test_validation_score():
    doc = RAGDocument(
        id="1",
        title="Длинный заголовок статьи",
        url="https://example.com/1",
        language="ru",
        source="Test",
        abstract="Достаточно длинная аннотация для прохождения валидации качества.",
        full_text_chunks=["Чанк с содержанием " * 50],
        processing_info=ProcessingInfo(),
    )
    score = compute_validation_score(doc)
    assert 0 <= score <= 1
    assert score >= 0.5


def test_validate_document_ok():
    doc = RAGDocument(
        id="1",
        title="Заголовок",
        url="https://x.com",
        language="ru",
        source="X",
        abstract="Аннотация",
    )
    ok, errors = validate_document(doc)
    assert ok is True
    assert len(errors) == 0


def test_validate_document_fail():
    doc = RAGDocument(
        id="1",
        title="",
        url="",
        language="ru",
        source="X",
    )
    ok, errors = validate_document(doc)
    assert ok is False
    assert len(errors) > 0


def test_content_hash_dedup():
    doc = RAGDocument(
        id="1",
        title="Same",
        url="https://same.com",
        language="ru",
        source="X",
        abstract="Same abstract",
    )
    h1 = content_hash(doc)
    h2 = content_hash(doc)
    assert h1 == h2
