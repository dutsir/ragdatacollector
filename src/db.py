"""Подключение к PostgreSQL и схема таблиц (задачи, документы)."""
from __future__ import annotations

import json
import os
from typing import Any, List, Optional

# Конфиг: DATABASE_URL из env или config/settings.yaml -> database.url
def _get_database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return url
    try:
        from pathlib import Path
        import yaml
        # Путь к config/settings.yaml от корня проекта (р src/) xnjjf ghfjdncj rfr nj xnj,s ghjdfg rfrghbdtnrf
        root = Path(__file__).resolve().parent.parent
        config_path = root / "config" / "settings.yaml"
        if not config_path.exists():
            return ""
        with open(config_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        db = data.get("database") or {}
        url = db.get("url") or ""
        return url.strip() if isinstance(url, str) else ""
    except Exception:
        return ""


DATABASE_URL = _get_database_url()

# Пул соединений (создаётся при старте приложения)
_pool: Any = None


async def get_pool():
    """Возвращает пул asyncpg (или None, если БД не настроена(ПОТМО НАДО ЮУДЕТ ДОПИСТАТЬ БД ))."""
    global _pool
    return _pool


async def init_db() -> bool:
    """Создаёт пул и таблицы. Возвращает True, е dhfg ffg fjfr ghjсли БД подключена."""
    global _pool
    if not DATABASE_URL:
        return False
    try:
        import asyncpg
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=60,
        )
        await _create_tables(_pool)
        return True
    except Exception:
        _pool = None
        return False


async def close_db() -> None:
    """Закрывает пул соединений."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def _create_tables(pool) -> None:
    """Создаёт таблицы tasks и documents при первом запуске."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id UUID PRIMARY KEY,
                status VARCHAR(32) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                request JSONB NOT NULL,
                result_count INT NOT NULL DEFAULT 0,
                error TEXT,
                documents_path TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                doc_id VARCHAR(128) NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                language VARCHAR(16),
                source VARCHAR(64),
                authors JSONB DEFAULT '[]',
                date VARCHAR(32),
                doi VARCHAR(256),
                abstract TEXT DEFAULT '',
                full_text_chunks JSONB DEFAULT '[]',
                files JSONB DEFAULT '[]',
                metadata JSONB DEFAULT '{}',
                processing_info JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_documents_task_id ON documents(task_id)
        """)


async def task_create(task_id: str, request_json: dict) -> None:
    """Добавляет задачу в БД."""
    pool = await get_pool()
    if not pool:
        return
    from datetime import datetime
    now = datetime.utcnow()
    request_str = json.dumps(request_json, ensure_ascii=False, default=str)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tasks (id, status, created_at, updated_at, request, result_count, error, documents_path)
            VALUES ($1::uuid, $2, $3, $4, $5::jsonb, 0, NULL, NULL)
            ON CONFLICT (id) DO NOTHING
            """,
            task_id,
            "pending",
            now,
            now,
            request_str,
        )


async def task_get(task_id: str, with_documents_preview: bool = True, documents_preview_limit: int = 10) -> Optional[dict]:
    """Возвращает задачу по id или None. При with_documents_preview подгружает превью документов из БД."""
    pool = await get_pool()
    if not pool:
        return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, status, created_at, updated_at, request, result_count, error, documents_path FROM tasks WHERE id = $1::uuid",
            task_id,
        )
    if not row:
        return None
    out = {
        "task_id": str(row["id"]),
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "request": row["request"],
        "result_count": row["result_count"] or 0,
        "error": row["error"],
        "documents_path": row["documents_path"],
        "documents": None,
    }
    if with_documents_preview and out["result_count"]:
        out["documents"] = await documents_get_by_task_id(task_id, limit=documents_preview_limit)
    return out


STALE_RUNNING_DEFAULT_SEC = 300  # для периодической проверки: running дольше 5 мин


async def tasks_mark_stale_running_failed(older_than_seconds: Optional[int] = None) -> int:
    """
    Помечает задачи в статусе running как failed.
    - older_than_seconds=None: все running (при старте сервера — процесс убит, все running зомби).
    - older_than_seconds=N: только те, у кого updated_at старше N секунд.
    Возвращает количество обновлённых строк.
    """
    pool = await get_pool()
    if not pool:
        return 0
    from datetime import datetime
    async with pool.acquire() as conn:
        if older_than_seconds is None:
            result = await conn.execute(
                """
                UPDATE tasks
                SET status = 'failed', updated_at = $1,
                    error = 'Server restarted or task abandoned (stale running)'
                WHERE status = 'running'
                """,
                datetime.utcnow(),
            )
        else:
            result = await conn.execute(
                """
                UPDATE tasks
                SET status = 'failed', updated_at = NOW(),
                    error = 'Server restarted or task abandoned (stale running)'
                WHERE status = 'running'
                  AND updated_at < NOW() - $1::interval
                """,
                f"{older_than_seconds} seconds",
            )
    try:
        return int(result.split()[-1]) if result else 0
    except (ValueError, IndexError):
        return 0


async def task_update_status(
    task_id: str,
    status: str,
    *,
    result_count: Optional[int] = None,
    error: Optional[str] = None,
    documents_path: Optional[str] = None,
) -> None:
    """Обновляет статус и опционально result_count, error, documents_path."""
    pool = await get_pool()
    if not pool:
        return
    from datetime import datetime
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE tasks SET status = $2, updated_at = $3, result_count = COALESCE($4, result_count),
            error = COALESCE($5, error), documents_path = COALESCE($6, documents_path)
            WHERE id = $1::uuid
            """,
            task_id,
            status,
            datetime.utcnow(),
            result_count,
            error,
            documents_path,
        )


async def documents_save(task_id: str, documents: List[dict]) -> None:
    """Сохраняет документы по task_id (предварительно удаляет старые)."""
    pool = await get_pool()
    if not pool or not documents:
        return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM documents WHERE task_id = $1::uuid", task_id)
        for d in documents:
            await conn.execute(
                """
                INSERT INTO documents (task_id, doc_id, title, url, language, source, authors, date, doi, abstract,
                full_text_chunks, files, metadata, processing_info)
                VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb)
                """,
                task_id,
                d.get("id", ""),
                d.get("title", ""),
                d.get("url", ""),
                d.get("language", ""),
                d.get("source", ""),
                json.dumps(d.get("authors", []), ensure_ascii=False, default=str),
                d.get("date"),
                d.get("doi"),
                d.get("abstract", ""),
                json.dumps(d.get("full_text_chunks", []), ensure_ascii=False, default=str),
                json.dumps(d.get("files", []), ensure_ascii=False, default=str),
                json.dumps(d.get("metadata", {}), ensure_ascii=False, default=str),
                json.dumps(d.get("processing_info", {}), ensure_ascii=False, default=str),
            )


async def documents_get_by_task_id(task_id: str, limit: Optional[int] = None) -> List[dict]:
    """Возвращает документы задачи (все или limit для превью)."""
    pool = await get_pool()
    if not pool:
        return []
    async with pool.acquire() as conn:
        if limit:
            rows = await conn.fetch(
                """
                SELECT doc_id, title, url, language, source, authors, date, doi, abstract,
                full_text_chunks, files, metadata, processing_info
                FROM documents WHERE task_id = $1::uuid ORDER BY id LIMIT $2
                """,
                task_id,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT doc_id, title, url, language, source, authors, date, doi, abstract,
                full_text_chunks, files, metadata, processing_info
                FROM documents WHERE task_id = $1::uuid ORDER BY id
                """,
                task_id,
            )
    def _ensure_native(val, default):
        """JSONB приходит как list/dict; если по какой-то причине str — парсим."""
        if val is None:
            return default
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return default
        return val

    return [
        {
            "id": r["doc_id"],
            "title": r["title"],
            "url": r["url"],
            "language": r["language"],
            "source": r["source"],
            "authors": _ensure_native(r["authors"], []),
            "date": r["date"],
            "doi": r["doi"],
            "abstract": r["abstract"] or "",
            "full_text_chunks": _ensure_native(r["full_text_chunks"], []),
            "files": _ensure_native(r["files"], []),
            "metadata": _ensure_native(r["metadata"], {}),
            "processing_info": _ensure_native(r["processing_info"], {}),
        }
        for r in rows
    ]
