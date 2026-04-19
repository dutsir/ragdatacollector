"""Семантическое чанкование по абзацам и токен-based fallback для RAG.

Этапы (от простого к эффективному):
1. Базовое: разбиение по абзацам/предложениям (split_into_sentences, _split_into_paragraphs).
2. Перекрытие: overlap_tokens / overlap_sentences — контекст на границах не теряется.
3. Структурное: учёт заголовков (##, 1. 2.) в _split_by_headings_then_paragraphs.
4. Семантическое на эмбеддингах: граница при смене темы (semantic_chunk_text_embeddings).
   Конфиг: use_embedding_chunker, similarity_threshold, embedding_model (all-MiniLM-L6-v2).

Рекомендации по настройке:
- По умолчанию: чанкование по абзацам с перекрытием (этапы 1–3), без тяжёлых зависимостей.
- Для этапа 4: pip install sentence-transformers; в config use_embedding_chunker: true.
- similarity_threshold 0.4–0.6: ниже — крупнее чанки, выше — мельче (чаще смена темы).
- Модель all-MiniLM-L6-v2 — быстрая и компактная; для мультиязыка можно использовать
  paraphrase-multilingual-MiniLM-L12-v2 (больше размер, лучше для ru/en).
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional

# Конфиг по умолчанию
DEFAULT_CHUNK_MIN = 500
DEFAULT_CHUNK_MAX = 2000
DEFAULT_OVERLAP = 100
# Целевой размер чанка для семантического режима (токенов)
DEFAULT_SEMANTIC_CHUNK_TARGET = 1500

# Строки/паттерны метаданных — убираем из текста перед чанкованием (для любого источника)
_CHUNK_JUNK_PATTERNS = (
    "URL статьи:",
    "Ссылка для цитирования",
    "ссылка для цитирования",
    "https://mir-nauki.com",
    "Мир науки.",
    "World of Science.",
)

# Начала абзацев, с которых не должен начинаться чанк (союз в начале мысли)
_CONJUNCTION_STARTS = (
    "и ",
    "но ",
    "однако ",
    "а ",
    "а также ",
    "также ",
    "however ",
    "but ",
    "and ",
    "also ",
)


def _normalize_input_text(text: str) -> str:
    """Нормализация текста из PDF (\\f, \\r, номера страниц) перед чанкованием."""
    try:
        from .text_clean import normalize_pdf_text
        return normalize_pdf_text(text)
    except ImportError:
        return text.replace("\f", " ").replace("\r", " ")


def clean_text_for_chunking(text: str) -> str:
    """
    Удаляет из текста строки с метаданными издателя/агрегатора,
    чтобы чанки содержали только основной текст статьи.
    Ожидается, что перед вызовом текст уже нормализован (normalize_pdf_text / _normalize_input_text).
    """
    if not text or not text.strip():
        return text
    lines = text.split("\n")
    out = []
    for line in lines:
        s = line.strip()
        if not s:
            out.append(line)
            continue
        if any(pat in s for pat in _CHUNK_JUNK_PATTERNS):
            continue
        if re.match(r"^https?://\S+$", s) and len(s) < 400:
            continue
        out.append(line)
    return "\n".join(out).strip()


def _dedupe_consecutive_paragraphs(paragraphs: list[str]) -> list[str]:
    """Удаляет подряд идущие одинаковые абзацы (дубликаты)."""
    if not paragraphs:
        return []
    out = [paragraphs[0]]
    for p in paragraphs[1:]:
        if p.strip() and p.strip() != (out[-1].strip() if out else ""):
            out.append(p)
    return out


# Абзац начинается с маленькой буквы (латиница/кириллица) — обрыв слова/фразы из PDF
_CONTINUATION_START = re.compile(r"^[a-zа-яё]")


def _merge_continuation_paragraphs(paragraphs: list[str], max_continuation_len: int = 400) -> list[str]:
    """
    Склеивает абзац, который начинается с маленькой буквы (обрыв слова/фразы из PDF),
    с предыдущим абзацем. Не трогает длинные абзацы — это может быть намеренное начало.
    """
    if not paragraphs or len(paragraphs) <= 1:
        return list(paragraphs)
    out = [paragraphs[0]]
    for p in paragraphs[1:]:
        s = p.strip()
        if not s:
            continue
        if len(s) <= max_continuation_len and _CONTINUATION_START.match(s):
            out[-1] = (out[-1].rstrip() + " " + s).strip()
        else:
            out.append(p)
    return out


def _split_into_paragraphs(text: str) -> list[str]:
    """Разбивает текст на абзацы по двойному переносу, затем по одиночному. Пустые отбрасываются."""
    text = text.strip()
    if not text:
        return []
    if "\n\n" in text:
        parts = re.split(r"\n\n+", text)
    else:
        parts = text.split("\n")
    paras = [p.strip() for p in parts if p.strip()]
    return _dedupe_consecutive_paragraphs(paras)


# Сокращения, после которых не разбивать предложение (рус./англ.)
_SENTENCE_NO_SPLIT_AFTER = re.compile(
    r"(?i)(?:т\.?\s*д\.?|т\.?\s*п\.?|др\.?|проф\.?|г\.?|гг\.?|стр\.?|рис\.?|no\.?|vol\.?|fig\.?|al\.?|etc\.?)\s*$"
)


def split_into_sentences(text: str) -> list[str]:
    """
    Разбивает текст на предложения по границам .!?… (этап 1: грамматические единицы).
    Не разбивает после типичных сокращений (т.д., др., No., Fig. и т.п.).
    """
    if not text or not text.strip():
        return []
    # Разбивка по . ! ? … при условии пробела/переноса/конца строки после
    raw = re.split(r"(?<=[.!?…])\s+", text)
    sentences = []
    for s in raw:
        s = s.strip()
        if not s:
            continue
        # Если предыдущее "предложение" кончается на сокращение — склеить с текущим
        if sentences and _SENTENCE_NO_SPLIT_AFTER.search(sentences[-1]):
            sentences[-1] = (sentences[-1] + " " + s).strip()
        else:
            sentences.append(s)
    return sentences


def _paragraph_starts_with_conjunction(para: str) -> bool:
    """Абзац начинается с союза — нежелательно как начало чанка."""
    s = para.strip()
    if not s:
        return False
    lower = s.lower()
    return any(lower.startswith(c) for c in _CONJUNCTION_STARTS)


def trim_to_sentence_boundary(text: str, min_length: int = 50) -> str:
    """
    Обрезает текст до последней границы предложения (.!?…) или до последнего пробела.
    Не допускает обрыва на середине слова (например «развитием эмф» -> «развитием» или до последнего предложения).
    """
    if not text or len(text) < min_length:
        return text.strip()
    t = text.rstrip()
    if not t:
        return text.strip()
    if t[-1] in ".!?…":
        return t
    # Ищем последнее вхождение конца предложения
    for i in range(len(t) - 1, min_length - 1, -1):
        if t[i] in ".!?…" and (i + 1 >= len(t) or t[i + 1].isspace() or t[i + 1] in "\n\r"):
            return t[: i + 1].rstrip()
    # Иначе до последнего пробела (не обрывать слово)
    last_space = t.rfind(" ")
    if last_space >= min_length:
        return t[: last_space + 1].rstrip()
    return t


def _split_by_headings_then_paragraphs(text: str) -> list[str]:
    """
    Разбивает текст на блоки по заголовкам (##, 1. 2. или номерам раздела), затем каждый блок по абзацам.
    Возвращает плоский список абзацев (заголовки объединены с первым абзацем блока или отдельно).
    """
    if not text.strip():
        return []
    # Разделители: двойной перенос + строка, начинающаяся с номера раздела или ##
    section_split = re.split(r"\n\n+(?=\d+[.)]\s+|\d+\.\s+[А-ЯA-Z]|##\s+|\*\*[А-ЯA-Z])", text)
    paragraphs = []
    for block in section_split:
        block = block.strip()
        if not block:
            continue
        if "\n\n" in block:
            for p in re.split(r"\n\n+", block):
                p = p.strip()
                if p:
                    paragraphs.append(p)
        else:
            paragraphs.append(block)
    return paragraphs if paragraphs else _split_into_paragraphs(text)


def _get_encoding():
    try:
        import tiktoken
        return tiktoken.get_encoding("cl100k_base")
    except Exception:
        return None


def count_tokens(text: str, encoding_name: str = "cl100k_base") -> int:
    """Подсчёт токенов (tiktoken). Fallback: приближение по словам."""
    enc = _get_encoding()
    if enc:
        return len(enc.encode(text))
    return len(text.split())  # грубая оценка


# Макс. токенов на один документ — чтобы не было MemoryError на огромных статьях
MAX_INPUT_TOKENS = 120_000
# Макс. чанков на документ — меньше памяти
MAX_CHUNKS_PER_DOCUMENT = 25


def _split_oversized_paragraph(para: str, max_tokens: int, count_tokens_fn) -> list[str]:
    """Если абзац не влезает в один чанк, режем по границам предложений."""
    n = count_tokens_fn(para)
    if n <= max_tokens:
        return [para]
    # Разбивка по концу предложения: . ! ? затем пробел/перенос
    sentences = re.split(r"(?<=[.!?])\s+", para)
    return [s.strip() for s in sentences if s.strip()]


def semantic_chunk_text(
    text: str,
    chunk_size_target: int = DEFAULT_SEMANTIC_CHUNK_TARGET,
    chunk_size_max: int = DEFAULT_CHUNK_MAX,
    overlap_tokens: int = DEFAULT_OVERLAP,
    overlap_paragraphs: int = 2,
    max_chunks: Optional[int] = None,
    max_input_tokens: Optional[int] = None,
    count_tokens_fn=None,
) -> list[str]:
    """
    Семантическое чанкование: по абзацам, без обрыва предложений.
    - Группирует абзацы в чанки до ~chunk_size_target токенов.
    - Перекрытие: в начало следующего чанка попадают последние 1–2 абзаца предыдущего.
    - Чанк не начинается с союза (и, но, однако) — такой абзац остаётся в предыдущем чанке.
    """
    if count_tokens_fn is None:
        count_tokens_fn = count_tokens
    if max_input_tokens is None:
        max_input_tokens = MAX_INPUT_TOKENS

    # Сначала по заголовкам/разделам (##, 1. 2. ), затем по абзацам
    if re.search(r"\n\n+\d+[.)]\s|\n\n+##\s", text):
        paragraphs = _split_by_headings_then_paragraphs(text)
    else:
        paragraphs = _split_into_paragraphs(text)
    paragraphs = _merge_continuation_paragraphs(paragraphs)
    if not paragraphs:
        return []

    # Элементы: абзацы или предложения из слишком длинных абзацев
    elements: list[str] = []
    for p in paragraphs:
        pt = count_tokens_fn(p)
        if pt <= chunk_size_max:
            elements.append(p)
        else:
            for sent in _split_oversized_paragraph(p, chunk_size_max, count_tokens_fn):
                if sent:
                    elements.append(sent)

    if not elements:
        return []
    if len(elements) == 1 and count_tokens_fn(elements[0]) <= chunk_size_max:
        return [elements[0]]

    chunks: list[str] = []
    current: list[str] = []
    current_tokens = 0
    total_tokens = 0

    i = 0
    while i < len(elements):
        el = elements[i]
        el_tokens = count_tokens_fn(el)
        total_tokens += el_tokens
        if total_tokens > max_input_tokens:
            break

        # Решение: закрыть текущий чанк и начать новый?
        would_be = current_tokens + el_tokens
        if current and would_be > chunk_size_max:
            # Следующий элемент начнёт новый чанк. Не начинать чанк с союза.
            if _paragraph_starts_with_conjunction(el) and current:
                # Оставляем этот элемент в текущем чанке (переполним чуть)
                current.append(el)
                current_tokens += el_tokens
                i += 1
                continue
            chunk_str = trim_to_sentence_boundary("\n\n".join(current).strip())
            if chunk_str:
                chunks.append(chunk_str)
            if max_chunks and len(chunks) >= max_chunks:
                break
            # Overlap: последние 1–2 элемента в начало следующего чанка
            overlap: list[str] = []
            overlap_tok = 0
            for j in range(len(current) - 1, -1, -1):
                if overlap_tok >= overlap_tokens and len(overlap) >= overlap_paragraphs:
                    break
                overlap.insert(0, current[j])
                overlap_tok += count_tokens_fn(current[j])
                if overlap_tok >= overlap_tokens:
                    break
            current = list(overlap)
            current_tokens = overlap_tok
            # Текущий элемент el переносим в новый чанк (уже не в overlap)
            current.append(el)
            current_tokens += el_tokens
            i += 1
            continue

        current.append(el)
        current_tokens += el_tokens
        i += 1

    if current and (not max_chunks or len(chunks) < max_chunks):
        chunk_str = trim_to_sentence_boundary("\n\n".join(current).strip())
        if chunk_str:
            chunks.append(chunk_str)

    return chunks[:max_chunks] if max_chunks else chunks


def semantic_chunk_text_embeddings(
    text: str,
    similarity_threshold: float = 0.5,
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    chunk_size_max: int = DEFAULT_CHUNK_MAX,
    chunk_size_min: int = DEFAULT_CHUNK_MIN,
    overlap_sentences: int = 2,
    max_chunks: Optional[int] = None,
    max_input_tokens: Optional[int] = None,
    count_tokens_fn: Optional[Callable[[str], int]] = None,
) -> Optional[list[str]]:
    """
    Семантическое чанкование на эмбеддингах (этап 4): граница при смене темы.
    Разбивает на предложения → эмбеддинги → косинусное сходство между соседними →
    граница чанка, когда сходство < similarity_threshold.
    Требует: pip install sentence-transformers (опционально).
    При ошибке или отсутствии модели возвращает None (используйте fallback).
    """
    if count_tokens_fn is None:
        count_tokens_fn = count_tokens
    if max_input_tokens is None:
        max_input_tokens = MAX_INPUT_TOKENS

    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None

    text = text.strip()
    if not text:
        return []
    sentences = split_into_sentences(text)
    sentences = [s for s in sentences if len(s.strip()) >= 10]
    if len(sentences) <= 1:
        return None

    try:
        model = SentenceTransformer(embedding_model)
        embeddings = model.encode(sentences, show_progress_bar=False)
    except Exception:
        return None

    try:
        import numpy as np
    except ImportError:
        return None

    # Косинусное сходство между соседними предложениями
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1e-9
    emb_norm = embeddings / norms
    sims = np.sum(emb_norm[:-1] * emb_norm[1:], axis=1)

    # Границы чанков: где сходство падает ниже порога (или индекс 0 не ставим)
    break_before = [False] * len(sentences)
    for i in range(1, len(sentences)):
        if sims[i - 1] < similarity_threshold:
            break_before[i] = True

    # Собрать чанки: группы предложений между границами, не превышая chunk_size_max
    chunks: list[str] = []
    current: list[str] = []
    current_tokens = 0

    for i, sent in enumerate(sentences):
        tok = count_tokens_fn(sent)
        if current_tokens + tok > max_input_tokens:
            if current and (not max_chunks or len(chunks) < max_chunks):
                chunk_str = trim_to_sentence_boundary(" ".join(current).strip())
                if chunk_str:
                    chunks.append(chunk_str)
            break
        if break_before[i] and current:
            chunk_str = trim_to_sentence_boundary(" ".join(current).strip())
            if chunk_str and count_tokens_fn(chunk_str) >= chunk_size_min:
                chunks.append(chunk_str)
                if max_chunks and len(chunks) >= max_chunks:
                    return chunks[:max_chunks]
            # Перекрытие: последние overlap_sentences в начало следующего чанка
            overlap = current[-overlap_sentences:] if len(current) >= overlap_sentences else current
            current = list(overlap)
            current_tokens = sum(count_tokens_fn(s) for s in current)
        current.append(sent)
        current_tokens += tok

        if current_tokens >= chunk_size_max:
            chunk_str = trim_to_sentence_boundary(" ".join(current).strip())
            if chunk_str:
                chunks.append(chunk_str)
                if max_chunks and len(chunks) >= max_chunks:
                    return chunks[:max_chunks]
            overlap = current[-overlap_sentences:] if len(current) >= overlap_sentences else current
            current = list(overlap)
            current_tokens = sum(count_tokens_fn(s) for s in current)

    if current and (not max_chunks or len(chunks) < max_chunks):
        chunk_str = trim_to_sentence_boundary(" ".join(current).strip())
        if chunk_str:
            chunks.append(chunk_str)

    return chunks[:max_chunks] if max_chunks else chunks


def compute_chunk_info(
    chunks: List[str],
    strategy: str = "semantic_paragraph",
    count_tokens_fn=None,
) -> Dict[str, Any]:
    """Метаданные о чанках: количество, размеры в токенах, стратегия."""
    if count_tokens_fn is None:
        count_tokens_fn = count_tokens
    if not chunks:
        return {
            "total_chunks": 0,
            "avg_tokens_per_chunk": 0,
            "min_tokens": 0,
            "max_tokens": 0,
            "chunking_strategy": strategy,
        }
    token_counts = [count_tokens_fn(c) for c in chunks]
    return {
        "total_chunks": len(chunks),
        "avg_tokens_per_chunk": round(sum(token_counts) / len(token_counts), 0),
        "min_tokens": min(token_counts),
        "max_tokens": max(token_counts),
        "chunking_strategy": strategy,
    }


def chunk_text(
    text: str,
    chunk_size_min: int = DEFAULT_CHUNK_MIN,
    chunk_size_max: int = DEFAULT_CHUNK_MAX,
    overlap_tokens: int = DEFAULT_OVERLAP,
    tokenizer: str = "tiktoken",
    max_input_tokens: Optional[int] = None,
    max_chunks: Optional[int] = None,
    use_embedding_chunker: bool = False,
    similarity_threshold: float = 0.5,
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
) -> list[str]:
    """
    Семантическое чанкование по абзацам (законченные мысли, перекрытие).
    Сначала нормализуется текст из PDF (\\f, \\r, номера страниц), затем удаляется мусор.
    Если use_embedding_chunker=True и установлен sentence-transformers — сначала
    пробуем чанкование по эмбеддингам (граница при смене темы). Иначе по абзацам/токенам.
    Чанки 500–2000 токенов, без обрыва предложений, без начала с союза (и, но, однако).
    """
    if not text or not text.strip():
        return []
    text = _normalize_input_text(text)
    text = clean_text_for_chunking(text)
    if not text.strip():
        return []

    if max_input_tokens is None:
        max_input_tokens = MAX_INPUT_TOKENS

    # Этап 4: семантическое чанкование на эмбеддингах (опционально)
    if use_embedding_chunker:
        emb_chunks = semantic_chunk_text_embeddings(
            text,
            similarity_threshold=similarity_threshold,
            embedding_model=embedding_model,
            chunk_size_max=chunk_size_max,
            chunk_size_min=chunk_size_min,
            overlap_sentences=2,
            max_chunks=max_chunks,
            max_input_tokens=max_input_tokens,
        )
        if emb_chunks:
            return emb_chunks

    paragraphs = _split_into_paragraphs(text)
    # Используем семантическое чанкование по абзацам, если есть структура
    if len(paragraphs) >= 2 or "\n\n" in text:
        semantic = semantic_chunk_text(
            text,
            chunk_size_target=max(chunk_size_min, (chunk_size_min + chunk_size_max) // 2),
            chunk_size_max=chunk_size_max,
            overlap_tokens=overlap_tokens,
            overlap_paragraphs=2,
            max_chunks=max_chunks,
            max_input_tokens=max_input_tokens,
        )
        if semantic:
            return semantic

    # Fallback: один блок без абзацев — режем по токенам с попыткой по границе предложения
    enc = _get_encoding() if tokenizer == "tiktoken" else None

    def tokenize(s: str):
        if enc:
            return enc.encode(s)
        return s.split()

    def detokenize(tokens) -> str:
        if enc:
            return enc.decode(list(tokens))
        return " ".join(tokens)

    tokens = tokenize(text)
    if len(tokens) > max_input_tokens:
        tokens = tokens[:max_input_tokens]
    if len(tokens) <= chunk_size_max:
        chunk = trim_to_sentence_boundary(detokenize(tokens).strip())
        return [chunk] if chunk else []

    chunks: list[str] = []
    start = 0
    while start < len(tokens):
        end = min(start + chunk_size_max, len(tokens))
        segment = tokens[start:end]
        chunk_str = detokenize(segment)
        if end < len(tokens) and len(segment) < chunk_size_min:
            take = min(chunk_size_min - len(segment), len(tokens) - end)
            if take > 0:
                segment = tokens[start : end + take]
                chunk_str = detokenize(segment)
                end = end + take

        if chunk_str.strip():
            chunks.append(trim_to_sentence_boundary(chunk_str.strip()))
        if max_chunks and len(chunks) >= max_chunks:
            break
        start = end - overlap_tokens
        if start >= len(tokens):
            break
        if start < 0:
            start = 0

    if max_chunks and len(chunks) > max_chunks:
        chunks = chunks[:max_chunks]
    return chunks
