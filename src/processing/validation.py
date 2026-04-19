"""Валидация качества данных и дедупликация."""
from __future__ import annotations

import hashlib
import re
from typing import Any

from ..models.document import RAGDocument

# Конечные знаки предложения (для проверки полноты текста)
SENTENCE_END_CHARS = ".!?…"


def normalize_date(value: str | None) -> str | None:
    """Приводит дату к формату YYYY-MM-DD. Принимает YYYY, YYYY-MM-DD, DD.MM.YYYY и т.п."""
    if not value or not value.strip():
        return None
    s = value.strip()
    # Уже YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    # Только год
    if re.match(r"^\d{4}$", s):
        return f"{s}-01-01"
    # DD.MM.YYYY или DD/MM/YYYY
    m = re.match(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})$", s)
    if m:
        d, mon, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{y}-{mon}-{d}"
    # YYYY-MM
    if re.match(r"^\d{4}-\d{2}$", s):
        return f"{s}-01"
    return None


def validate_text_ends_complete(text: str) -> tuple[bool, str]:
    """
    Проверяет, что текст заканчивается на границе предложения (не обрывается).
    Возвращает (ok, trimmed): ok=True если уже ок; иначе trimmed — обрезка до последнего предложения.
    """
    if not text or not text.strip():
        return (True, text)
    t = text.rstrip()
    if not t:
        return (True, text)
    if t[-1] in SENTENCE_END_CHARS:
        return (True, text)
    # Ищем последнее вхождение конца предложения
    last_end = -1
    for i in range(len(t) - 1, -1, -1):
        if t[i] in SENTENCE_END_CHARS and (i + 1 >= len(t) or t[i + 1].isspace() or t[i + 1] in "\n\r"):
            last_end = i
            break
    if last_end >= 50:
        return (False, t[: last_end + 1].rstrip())
    # Иначе до последнего пробела (не обрывать на середине слова)
    last_space = t.rfind(" ")
    if last_space > 50:
        return (False, t[: last_space + 1].rstrip())
    return (True, text)


def ensure_chunks_end_at_boundaries(chunks: list[str]) -> list[str]:
    """
    Убирает обрывы на середине слова/предложения в чанках.
    Каждый чанк обрезается до последнего конца предложения или до последнего пробела.
    """
    result = []
    for c in chunks:
        if not c or not c.strip():
            continue
        _, trimmed = validate_text_ends_complete(c)
        if trimmed.strip():
            result.append(trimmed.strip())
    return result


def chunk_hashes_for_dedupe(chunks: list[str], hash_len: int = 16) -> list[str]:
    """Хеши чанков для отсеивания дубликатов (например, simhash или обычный хеш)."""
    out = []
    for c in chunks:
        h = hashlib.sha256(c.strip().encode("utf-8")).hexdigest()[:hash_len]
        out.append(h)
    return out


def dedupe_chunks_by_hash(chunks: list[str], consecutive_only: bool = True) -> list[str]:
    """
    Удаляет дубликаты чанков по хешу.
    consecutive_only=True: только подряд идущие с одинаковым хешем (не трогает повторы в разных местах).
    consecutive_only=False: любые повторяющиеся чанки удаляются (остаётся первое вхождение).
    """
    if not chunks:
        return []
    result = []
    prev_hash = None
    seen = set()
    for c in chunks:
        h = hashlib.sha256(c.strip().encode("utf-8")).hexdigest()
        if consecutive_only:
            if h == prev_hash:
                continue
            prev_hash = h
        else:
            if h in seen:
                continue
            seen.add(h)
        result.append(c)
    return result


def _first_chunk_junk_ratio(chunks: list[str]) -> float:
    """Доля «мусорного» контента в первом чанке (URL статьи, ссылки и т.д.). 0 = чистый, 1 = весь мусор."""
    if not chunks or not chunks[0].strip():
        return 0.0
    first = chunks[0]
    junk_markers = (
        "URL статьи:",
        "Ссылка для цитирования",
        "https://mir-nauki.com",
        "Мир науки.",
        "World of Science.",
    )
    junk_len = 0
    for line in first.split("\n"):
        s = line.strip()
        if not s:
            continue
        if any(m in s for m in junk_markers):
            junk_len += len(line)
        elif s.startswith("http") and s.count("/") >= 3:
            junk_len += len(line)
    total = len(first.strip()) or 1
    return min(1.0, junk_len / total)


def compute_validation_score(doc: RAGDocument) -> float:
    """
    Оценка качества документа 0.0–1.0 по объективным метрикам:
    наличие полей, полнота аннотации, качество чанков (без мусора в первом чанке).
    """
    score = 0.0
    if doc.title and len(doc.title) >= 5:
        score += 0.2
    if doc.url:
        score += 0.2
    if doc.abstract and len(doc.abstract) >= 20:
        score += 0.15
        if len(doc.abstract) >= 100 and not doc.abstract.rstrip().endswith("..."):
            score += 0.05
    if doc.full_text_chunks:
        score += 0.2
        avg_len = sum(len(c) for c in doc.full_text_chunks) / len(doc.full_text_chunks)
        if avg_len >= 200:
            score += 0.2
        junk = _first_chunk_junk_ratio(doc.full_text_chunks)
        score -= 0.15 * junk
    else:
        if doc.abstract and len(doc.abstract) >= 100:
            score += 0.2
    return max(0.0, min(1.0, score))


def validate_document(doc: RAGDocument) -> tuple[bool, list[str]]:
    """
    Проверка документа на минимальные требования.
    Возвращает (ok, list of error messages).
    """
    errors: list[str] = []
    if not doc.title or len(doc.title) < 2:
        errors.append("title too short or missing")
    if not doc.url:
        errors.append("url missing")
    if not doc.source:
        errors.append("source missing")
    if not doc.language:
        errors.append("language missing")
    if not doc.abstract and not doc.full_text_chunks:
        errors.append("no abstract and no full_text_chunks")
    full_text_joined = " ".join(doc.full_text_chunks) if doc.full_text_chunks else ""
    if full_text_joined and full_text_joined.strip():
        ok_end, _ = validate_text_ends_complete(full_text_joined)
        if not ok_end:
            errors.append("full_text ends mid-sentence or mid-word (incomplete extraction)")
    if doc.date and normalize_date(doc.date) is None:
        errors.append("date format could not be normalized to YYYY-MM-DD")
    return (len(errors) == 0, errors)


def content_hash(doc: RAGDocument, fields: list[str] | None = None) -> str:
    """Хеш для дедупликации по указанным полям."""
    if fields is None:
        fields = ["title", "url", "abstract"]
    parts = []
    for f in fields:
        v = getattr(doc, f, None)
        if v is None:
            v = doc.metadata.get(f)
        if isinstance(v, list):
            v = "|".join(str(x) for x in v)
        parts.append(str(v or ""))
    return hashlib.sha256("".join(parts).encode("utf-8")).hexdigest()
