"""Text processing: chunking, validation, language detection, text cleaning."""
from .chunking import chunk_text, compute_chunk_info, split_into_sentences, semantic_chunk_text_embeddings
from .language import detect_language
from .text_clean import (
    clean_text,
    clean_text_for_rag,
    clean_text_preserving_structure,
    clean_text_multilingual,
    clean_text_with_logging,
    preserve_tables_and_formulas,
    document_type_from_text,
    extract_metadata_from_pdf_text,
    filter_and_clean_chunks,
    is_dissertation,
    normalize_pdf_text,
    detect_languages,
    CleaningConfig,
    process_text_with_config,
)
from .validation import validate_document, compute_validation_score

__all__ = [
    "chunk_text",
    "compute_chunk_info",
    "split_into_sentences",
    "semantic_chunk_text_embeddings",
    "detect_language",
    "validate_document",
    "compute_validation_score",
    "clean_text",
    "clean_text_for_rag",
    "clean_text_preserving_structure",
    "clean_text_multilingual",
    "clean_text_with_logging",
    "preserve_tables_and_formulas",
    "normalize_pdf_text",
    "extract_metadata_from_pdf_text",
    "filter_and_clean_chunks",
    "is_dissertation",
    "document_type_from_text",
    "detect_languages",
    "CleaningConfig",
    "process_text_with_config",
]
