"""Очистка и нормализация текста перед чанкированием. Метаданные из PDF."""
from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Паттерны строк, которые считаем служебными (номера страниц, том/выпуск журнала)
_PAGE_ISSUE_PATTERNS = re.compile(
    r"^\s*(?:Том\s*\d+|№\s*\d+|Vol\.\s*\d+|N\.\s*\d+|\d+\s*,\s*№\s*\d+)\s*[\f\n]",
    re.IGNORECASE | re.MULTILINE,
)

# Паттерны колонтитулов журналов (только обобщённые: номера страниц, ISSN, DOI и т.п.)
_JOURNAL_HEADER_PATTERNS = [
    re.compile(r"^\d{1,5}\s*$"),
    re.compile(r"^стр\.?\s*\d+$", re.IGNORECASE),
    re.compile(r"^p\.?\s*\d+$", re.IGNORECASE),
    re.compile(r"^том\s*\d+.*выпуск\s*\d+$", re.IGNORECASE),
    re.compile(r"^vol\.?\s*\d+.*no\.?\s*\d+$", re.IGNORECASE),
    re.compile(r"^\d+\s*,\s*№?\s*\d+$"),
    re.compile(r"issn\s*\d{4}-\d{4}", re.IGNORECASE),
    re.compile(r"doi:\s*10\.\d{4,9}/", re.IGNORECASE),
    re.compile(r"^www\.[a-z0-9.-]+\.[a-z]{2,}$", re.IGNORECASE),
    re.compile(r"^\S+@\S+\.\S+$"),
    re.compile(r"©|copyright|all rights reserved", re.IGNORECASE),
    re.compile(r"volume\s+\d+\s+issue\s+\d+", re.IGNORECASE),
    re.compile(r"journal of.*vol\.\s*\d+", re.IGNORECASE),
]


def _is_journal_header_line_impl(line: str) -> bool:
    """
    Определяет, является ли строка колонтитулом журнала (номера страниц, ISSN, DOI и т.п.).
    Длинные строки (>200 символов) не считаются колонтитулами.
    """
    s = line.strip()
    if len(s) < 2 or len(s) > 200:
        return False
    for pat in _JOURNAL_HEADER_PATTERNS:
        if pat.search(s):
            return True
    if len(s) >= 50:
        return False
    upper_count = sum(1 for c in s if c.isupper())
    digit_count = sum(1 for c in s if c.isdigit())
    if digit_count > 0 and digit_count / len(s) > 0.7:
        return True
    if upper_count / len(s) > 0.8:
        return True
    return False


@lru_cache(maxsize=1000)
def _is_journal_header_line_cached(line: str) -> bool:
    """Кэш только для коротких строк (колонтитулы обычно < 200 символов)."""
    return _is_journal_header_line_impl(line)


def _is_journal_header_line(line: str) -> bool:
    """Проверка колонтитула. Длинные строки не кэшируем — иначе зависания при больших чанках."""
    if len(line) > 300:
        return _is_journal_header_line_impl(line)
    return _is_journal_header_line_cached(line)


def _base_clean(text: str) -> str:
    """Базовая очистка: NFKC и удаление управляющих символов."""
    if not text or not isinstance(text, str):
        return ""
    t = unicodedata.normalize("NFKC", text)
    t = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", t)
    return t


def clean_text(text: str, preserve_structure: bool = True) -> str:
    """
    Очистка текста с опцией сохранения структуры.

    Args:
        text: Исходный текст.
        preserve_structure: Если True — сохраняются переносы строк (абзацы).
                            Если False — все переносы схлопываются в пробелы (одна строка).
    """
    if not text or not isinstance(text, str):
        return ""
    t = _base_clean(text)
    if preserve_structure:
        t = t.replace("\f", "\n\n")
        t = t.replace("\r\n", "\n").replace("\r", "\n")
        t = re.sub(r"\n{3,}", "\n\n", t)
        lines = []
        for line in t.split("\n"):
            line = re.sub(r"[ \t]+", " ", line).strip()
            if line:
                lines.append(line)
        return "\n".join(lines)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def clean_text_preserving_structure(text: str) -> str:
    """Очистка с сохранением структуры документа. Эквивалент clean_text(text, preserve_structure=True)."""
    return clean_text(text, preserve_structure=True)


def _join_hyphenated_line_breaks(text: str) -> str:
    """
    Склеивает переносы слов через дефис (типично для PDF):
    «echocardiogra-\\nphy» -> «echocardiography», «непре-\\nрывного» -> «непрерывного».
    """
    if not text or "\n" not in text:
        return text
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"-\s*\n\s*", "", t)
    return t


def normalize_pdf_text(text: str) -> str:
    """
    Нормализация текста из PDF: объединение переносов слов, удаление служебных строк
    (номера страниц, том/выпуск), затем очистка с сохранением структуры.
    """
    if not text or not text.strip():
        return ""
    t = _join_hyphenated_line_breaks(text)
    t = _PAGE_ISSUE_PATTERNS.sub(" ", t)
    t = t.replace("\f", "\n\n")
    return clean_text(t, preserve_structure=True).strip()


def clean_text_for_rag(text: str) -> str:
    """Очистка для RAG: нормализация PDF + схлопывание переносов (одна строка). Эквивалент clean_text(normalize_pdf_text(text), False)."""
    return clean_text(normalize_pdf_text(text), preserve_structure=False)


def clean_text_with_logging(text: str, source_url: str = "") -> str:
    """Очистка с сохранением структуры и логгированием для отладки."""
    original_length = len(text) if text else 0
    cleaned = clean_text(text, preserve_structure=True)
    cleaned_length = len(cleaned)
    if original_length > 0 and logger.isEnabledFor(logging.DEBUG):
        ratio = cleaned_length / original_length
        logger.debug(
            "Cleaned text from %s to %s chars (%.1f%%) for %s",
            original_length,
            cleaned_length,
            ratio * 100,
            source_url or "(no url)",
        )
    return cleaned


def preserve_tables_and_formulas(text: str) -> str:
    """
    Сохраняет таблицы и формулы: нормализует пробелы вокруг ±≈ и математических символов.
    Для научных статей важно не терять смысл формул.
    """
    if not text or not text.strip():
        return text
    # Пробелы вокруг ± ≈ в числах
    text = re.sub(r"(\d+)\s*([±≈~])\s*(\d+)", r"\1 \2 \3", text)
    text = re.sub(r"(\d+\.\d+)\s*([±])\s*(\d+\.\d+)", r"\1 \2 \3", text)
    math_replacements = {
        "≤": " <= ",
        "≥": " >= ",
        "≈": " ≈ ",
        "≠": " != ",
        "×": " × ",
        "÷": " ÷ ",
        "±": " ± ",
        "∆": " Δ ",
    }
    for symbol, replacement in math_replacements.items():
        text = text.replace(symbol, replacement)
    return text


def extract_metadata_from_pdf_text(pdf_text: str) -> Dict[str, Any]:
    """Извлечение метаданных из текста PDF (УДК, ВАК, благодарности)."""
    meta: Dict[str, Any] = {}
    if not pdf_text or not pdf_text.strip():
        return meta
    # УДК
    m = re.search(r"УДК\s+([\d.\s]+?)(?:\s|$|\n)", pdf_text)
    if m:
        meta["udc"] = m.group(1).strip()
    # Код ВАК
    m = re.search(r"Код\s+ВАК\s+([\d.]+)", pdf_text, re.IGNORECASE)
    if m:
        meta["vak_code"] = m.group(1).strip()
    # Благодарности (короткий фрагмент после "Благодарности.")
    m = re.search(
        r"Благодарности\.\s*(.+?)(?=\n\n|Для цитирования|Список литературы|References|$)",
        pdf_text,
        re.DOTALL | re.IGNORECASE,
    )
    if m:
        ack = re.sub(r"\s+", " ", m.group(1)).strip()
        if len(ack) < 2000:
            meta["acknowledgments"] = ack
    return meta


def is_dissertation(text: str) -> bool:
    """
    Определение, является ли документ диссертацией/авторефератом (по индикаторам).
    Ужесточённая логика: не помечаем как диссертацию при одном случайном упоминании слова.
    — Либо в начале текста (обычно заголовок) есть «диссертация»/«автореферат»;
    — либо в тексте не менее 2 разных сильных индикаторов.
    """
    if not text:
        return False
    lower = (text or "").lower()
    title_zone = lower[:400]

    strong_in_title = [
        "диссертация",
        "автореферат",
        "кандидатская диссертация",
        "докторская диссертация",
        "dissertation",
        "phd thesis",
        "doctoral thesis",
    ]
    if any(ind in title_zone for ind in strong_in_title):
        return True

    indicators = [
        "диссертация",
        "дисс.",
        "кандидатская",
        "докторская",
        "автореферат",
        "dissertation",
        "phd thesis",
        "doctoral thesis",
        "на соискание степени",
        "защищена в",
    ]
    found = [ind for ind in indicators if ind in lower]
    return len(found) >= 2


def document_type_from_text(title: str, abstract: str, full_text: str = "") -> str:
    """
    Определение типа документа с пороговыми значениями.
    Явные указания в заголовке имеют приоритет; иначе — взвешенные ключевые слова.
    """
    scores: Dict[str, float] = {"dissertation": 0.0, "conference_paper": 0.0, "scientific_article": 0.0}
    title_lower = (title or "").lower()
    abstract_lower = (abstract or "").lower()
    combined = title_lower + " " + abstract_lower

    # Явные указания типа в заголовке
    if any(x in title_lower for x in ["диссертация", "автореферат"]):
        return "dissertation"
    if any(x in title_lower for x in ["конференция", "conference", "сборник"]):
        return "conference_paper"  # тип для API

    # Взвешенные ключевые слова для диссертации
    for keyword, weight in [
        ("кандидатск", 2),
        ("докторск", 2),
        ("на соискание", 3),
        ("специальность", 1),
        ("защищена", 2),
        ("диссертационн", 1),
    ]:
        if keyword in combined:
            scores["dissertation"] += weight

    # Конференция
    for keyword, weight in [
        ("материалы конференции", 3),
        ("сборник трудов", 2),
        ("proceedings", 2),
        ("тезисы докладов", 2),
    ]:
        if keyword in combined:
            scores["conference_paper"] += weight

    # Структура текста: диссертации часто содержат "Глава N"
    if full_text:
        if re.search(r"глава\s+\d+|chapter\s+\d+", full_text[:2000], re.IGNORECASE):
            scores["dissertation"] += 1

    max_score = max(scores.values())
    if max_score == 0:
        return "scientific_article"
    return max(scores, key=scores.get)


def _is_meaningful_short_text(text: str) -> bool:
    """Определяет, является ли короткий текст осмысленным (заголовок, определение, ключевые слова)."""
    if not text or len(text.strip()) < 5:
        return False
    text_lower = text.lower().strip()
    meaningful_patterns = [
        r"^[А-ЯA-Z][^.!?]{5,50}[.!?:]?$",
        r"определени[ея]|понятие|термин",
        r"key\s+words|ключевые\s+слова",
        r"abstract|аннотация|резюме",
    ]
    for pattern in meaningful_patterns:
        if re.search(pattern, text_lower):
            return True
    sentences = re.split(r"[.!?]+", text)
    for sent in sentences:
        if len(sent.split()) >= 3:
            return True
    return False


def _contains_meaningful_content(lines: List[str]) -> bool:
    """Есть ли в строках осмысленный контент (цифры, буквы, пунктуация)."""
    joined = " ".join(lines)
    digit_count = sum(1 for c in joined if c.isdigit())
    letter_count = sum(1 for c in joined if c.isalpha())
    return digit_count >= 2 or letter_count >= 5


def _looks_like_structured_data(lines: List[str]) -> bool:
    """Похоже на таблицу/список (числа, разделители, короткие колонки)."""
    if len(lines) < 2:
        return False
    digit_ratio = sum(1 for c in " ".join(lines) if c.isdigit()) / max(1, sum(len(l) for l in lines))
    if digit_ratio > 0.2:
        return True
    if any(re.search(r"[\d.,;:\t]+", l) for l in lines):
        return True
    return False


def strip_journal_headers_from_chunk(chunk: str) -> str:
    """Удаляет из чанка строки-колонтитулы журналов."""
    if not chunk or not chunk.strip():
        return chunk
    lines = chunk.split("\n")
    out = [line for line in lines if not _is_journal_header_line(line)]
    return "\n".join(out).strip()


def is_junk_chunk(chunk: str, min_words: int = 15, min_chars: int = 80) -> bool:
    """
    Чанк считается мусорным, если: пустой; слишком короткий и не осмысленный;
    «вертикальный» мусор; слишком мало букв. Для формул/таблиц пороги мягче.
    """
    if not chunk or not chunk.strip():
        return True
    s = chunk.strip()
    if len(s) < min_chars:
        return not _is_meaningful_short_text(s)
    words = s.split()
    if len(words) < min_words:
        return not _is_meaningful_short_text(s)
    lines = [l.strip() for l in s.splitlines() if l.strip()]
    if not lines:
        return True
    if len(lines) > 5:
        avg_line_len = sum(len(l) for l in lines) / len(lines)
        if avg_line_len < 3 and not _contains_meaningful_content(lines):
            return True
    letter_count = sum(1 for c in s if c.isalpha())
    if letter_count < 10:
        return True
    digit_count = sum(1 for c in s if c.isdigit())
    symbol_count = len(s) - letter_count - digit_count
    if digit_count + symbol_count > len(s) * 0.8:
        return not _looks_like_structured_data(lines)
    if letter_count < len(s) * 0.25:
        return True
    return False


def filter_and_clean_chunks(
    chunks: List[str],
    config: Optional[CleaningConfig] = None,
) -> List[str]:
    """
    Фильтрация и очистка чанков с опциональной конфигурацией.
    Fallback: при пустом результате — менее строгая проверка (минимум 20 символов, 5 букв/цифр).
    """
    if not chunks:
        return []
    cfg = config or CleaningConfig()
    result = []
    for chunk in chunks:
        cleaned = strip_journal_headers_from_chunk(chunk)
        if not cleaned or is_junk_chunk(
            cleaned,
            min_words=cfg.min_chunk_words,
            min_chars=cfg.min_chunk_chars,
        ):
            continue
        result.append(cleaned)
    if result:
        return result
    # Fallback: менее строгая фильтрация, но с минимальным порогом качества
    for chunk in chunks:
        cleaned = strip_journal_headers_from_chunk(chunk)
        if not cleaned or len(cleaned.strip()) < 20:
            continue
        content_chars = sum(1 for c in cleaned if c.isalnum())
        if content_chars < 5:
            continue
        result.append(cleaned)
    return result


def detect_languages(text: str, max_sentences: int = 50) -> List[str]:
    """Определение языков в тексте (требует langdetect). Возвращает до 3 наиболее частых."""
    try:
        import langdetect
        from collections import Counter
    except ImportError:
        return []
    sentences = re.split(r"[.!?]+", text)
    languages: List[str] = []
    for sent in sentences[:max_sentences]:
        if len(sent.strip()) > 20:
            try:
                lang = langdetect.detect(sent)
                languages.append(lang)
            except Exception:
                pass
    return [lang for lang, _ in Counter(languages).most_common(3)]


def clean_text_multilingual(text: str) -> str:
    """Очистка с поддержкой Unicode: нормализация NFKC, удаление только управляющих символов."""
    if not text or not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFKC", text)
    cleaned = []
    for char in text:
        cat = unicodedata.category(char)
        if cat[0] != "C" or char in "\n\r\t":
            cleaned.append(char)
        else:
            cleaned.append(" ")
    text = "".join(cleaned)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


@dataclass
class CleaningConfig:
    """Конфигурация очистки текста под разные типы документов."""

    preserve_structure: bool = True
    min_chunk_chars: int = 80
    min_chunk_words: int = 15
    letter_ratio_threshold: float = 0.25
    remove_journal_headers: bool = True
    languages: Optional[List[str]] = None
    chunk_method: str = "paragraphs"  # "paragraphs" — разбивка по двойному переносу

    @classmethod
    def for_dissertations(cls) -> "CleaningConfig":
        return cls(
            preserve_structure=True,
            min_chunk_chars=60,
            min_chunk_words=10,
            letter_ratio_threshold=0.2,
            remove_journal_headers=False,
        )

    @classmethod
    def for_scientific_articles(cls) -> "CleaningConfig":
        return cls(
            preserve_structure=True,
            min_chunk_chars=100,
            min_chunk_words=20,
            letter_ratio_threshold=0.3,
            remove_journal_headers=True,
        )


def _chunk_text_by_paragraphs(text: str) -> List[str]:
    """Разбивка текста на чанки по абзацам (двойной перенос). Без зависимости от chunking.py."""
    if not text or not text.strip():
        return []
    return [p.strip() for p in text.split("\n\n") if p.strip()]


def process_text_with_config(text: str, config: CleaningConfig) -> Dict[str, Any]:
    """
    Полная обработка текста по конфигурации: очистка, опциональная мультиязычная нормализация,
    разбивка по абзацам, фильтрация чанков. Возвращает cleaned_text, chunks и chunk_stats.
    """
    if not text:
        return {
            "cleaned_text": "",
            "chunks": [],
            "chunk_stats": {"original_count": 0, "filtered_count": 0, "removed_ratio": 0.0},
            "min_chunk_chars": config.min_chunk_chars,
            "min_chunk_words": config.min_chunk_words,
            "letter_ratio_threshold": config.letter_ratio_threshold,
            "remove_journal_headers": config.remove_journal_headers,
        }
    cleaned = clean_text(text, preserve_structure=config.preserve_structure)
    if config.languages and any(lang not in ("ru", "en") for lang in config.languages):
        cleaned = clean_text_multilingual(cleaned)
    chunks = _chunk_text_by_paragraphs(cleaned)
    filtered = filter_and_clean_chunks(chunks, config)
    return {
        "cleaned_text": cleaned,
        "chunks": filtered,
        "chunk_stats": {
            "original_count": len(chunks),
            "filtered_count": len(filtered),
            "removed_ratio": (len(chunks) - len(filtered)) / max(1, len(chunks)),
        },
        "min_chunk_chars": config.min_chunk_chars,
        "min_chunk_words": config.min_chunk_words,
        "letter_ratio_threshold": config.letter_ratio_threshold,
        "remove_journal_headers": config.remove_journal_headers,
    }
