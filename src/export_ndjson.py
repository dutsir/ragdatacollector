"""Экспорт документов в NDJSON для загрузки в векторные БД."""
from __future__ import annotations

import json
from pathlib import Path
from typing import BinaryIO, TextIO

from .models.document import RAGDocument


def export_to_ndjson(
    documents: list[RAGDocument],
    path: str | Path | None = None,
    *,
    stream: TextIO | BinaryIO | None = None,
    include_processing_info: bool = True,
) -> str | None:
    """
    Экспорт в NDJSON. Либо запись в файл (path), либо в поток (stream).
    Возвращает путь к файлу при записи в path, иначе None.
    """
    if path is not None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        out: TextIO = open(path, "w", encoding="utf-8")
        own_stream = True
    elif stream is not None:
        out = stream if hasattr(stream, "write") and hasattr(stream, "encoding") else open(stream, "w", encoding="utf-8")
        own_stream = False
    else:
        return None

    try:
        for doc in documents:
            d = doc.model_dump(exclude_none=False)
            if not include_processing_info:
                d.pop("processing_info", None)
            line = json.dumps(d, ensure_ascii=False) + "\n"
            out.write(line)
        if path is not None:
            return str(path)
        return None
    finally:
        if own_stream and path is not None:
            out.close()


def load_ndjson(path: str | Path) -> list[dict]:
    """Загрузка NDJSON файла в список словарей."""
    path = Path(path)
    docs = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            docs.append(json.loads(line))
    return docs
