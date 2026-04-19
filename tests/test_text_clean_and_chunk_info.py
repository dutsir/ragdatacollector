"""Проверка очистки текста, чанк-инфо и типа документа."""
from src.processing.text_clean import (
    clean_text,
    clean_text_preserving_structure,
    normalize_pdf_text,
    is_dissertation,
    document_type_from_text,
    extract_metadata_from_pdf_text,
    detect_languages,
    clean_text_multilingual,
    CleaningConfig,
    process_text_with_config,
)
from src.processing.chunking import chunk_text, compute_chunk_info

def test_normalize_and_clean():
    t = "25, № 06\fтрадиционные  азотные, фосфорные\n\nТекст статьи."
    norm = normalize_pdf_text(t)
    assert "\f" not in norm
    assert "традиционные" in norm
    cl = clean_text(t)
    assert "\f" not in cl
    assert "традиционные" in cl and "Текст статьи" in cl

def test_is_dissertation():
    assert is_dissertation("Кандидатская диссертация") is True
    assert is_dissertation("Статья о климате") is False

def test_document_type():
    assert document_type_from_text("Диссертация", "", "") == "dissertation"
    assert document_type_from_text("Статья о чае", "Текст", "") == "scientific_article"

def test_extract_metadata():
    meta = extract_metadata_from_pdf_text("УДК 631.559.2\nКод ВАК 4.1.1\nТекст.")
    assert meta.get("udc") == "631.559.2"
    assert meta.get("vak_code") == "4.1.1"

def test_chunk_info():
    chunks = chunk_text("Первый абзац. Второе предложение.\n\nВторой абзац. Ещё предложение.")
    assert len(chunks) >= 1
    info = compute_chunk_info(chunks)
    assert info["total_chunks"] == len(chunks)
    assert "chunking_strategy" in info
    assert info["min_tokens"] >= 0


def test_clean_preserves_structure():
    t = "Строка один.\fСтрока два.\n\nАбзац два."
    out = clean_text_preserving_structure(t)
    assert "\f" not in out
    assert "Строка один" in out and "Строка два" in out
    assert out.count("\n") >= 1


def test_document_type_weighted():
    assert document_type_from_text("Диссертация по экономике", "", "") == "dissertation"
    assert document_type_from_text("Статья", "На соискание степени кандидата", "") == "dissertation"
    assert document_type_from_text("О климате", "Текст аннотации", "") == "scientific_article"


def test_multilingual_and_config():
    cleaned = clean_text_multilingual("  Текст  с   пробелами  \t\n  ")
    assert "Текст" in cleaned and "пробелами" in cleaned
    cfg = CleaningConfig.for_scientific_articles()
    result = process_text_with_config("Абзац один.\n\nАбзац два.", cfg)
    assert "cleaned_text" in result and result["min_chunk_chars"] == 100
    assert "chunks" in result and "chunk_stats" in result
    assert result["chunk_stats"]["original_count"] >= 1 and result["chunk_stats"]["filtered_count"] >= 0
