"""Очередь задач: PostgreSQL при наличии DATABASE_URL, иначе in-memory."""
from __future__ import annotations

import asyncio
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional
from uuid import uuid4

from .models.api import CollectRequest, TaskStatus
from .models.document import RAGDocument
from .collector import run_collection
from .export_ndjson import export_to_ndjson

from . import db as db_module


def _sync_run_collection(request_dict: dict) -> List[dict]:
    """
    Запускает полный сбор в отдельном потоке с собственным event loop,
    чтобы не блокировать основной цикл API — GET /tasks отвечает сразу.
    Возвращает list[dict] (model_dump документов).
    """
    import traceback as _tb
    request = CollectRequest.model_validate(request_dict)
    print(f"[collection] started sources={request.sources} keywords={request.keywords[:2] if request.keywords else []}...", flush=True)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        documents = loop.run_until_complete(run_collection(request))
        print(f"[collection] completed documents={len(documents)}", flush=True)
        return [d.model_dump(mode="json", exclude_none=False) for d in documents]
    except Exception as e:
        err = (str(e) or repr(e) or type(e).__name__).strip() or "Unknown error"
        print(f"[collection thread] Error: {err}\n{_tb.format_exc()}", flush=True)
        raise
    finally:
        loop.close()

def _get_collection_timeout() -> int:
    try:
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        config_path = root / "config" / "settings.yaml"
        if config_path.exists():
            import yaml
            with open(config_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            v = (data.get("app") or {}).get("collection_timeout_sec")
            if v is not None:
                return int(v)
    except Exception:
        pass
    return 1200


MAX_DOCS_IN_MEMORY = 10
_collection_semaphore = asyncio.Semaphore(1)


class TaskQueue:
    """Очередь задач: при подключённой БД — PostgreSQL, иначе in-memory."""

    def __init__(self) -> None:
        self._memory: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._exports_dir = Path("exports")

    async def _use_db(self) -> bool:
        return (await db_module.get_pool()) is not None

    async def create(self, request: CollectRequest) -> str:
        task_id = str(uuid4())
        now = datetime.utcnow()
        if await self._use_db():
            await db_module.task_create(task_id, request.model_dump())
            return task_id
        async with self._lock:
            self._memory[task_id] = {
                "task_id": task_id,
                "status": TaskStatus.pending,
                "created_at": now,
                "updated_at": now,
                "request": request,
                "result_count": 0,
                "documents": None,
                "documents_path": None,
                "error": None,
            }
        return task_id

    async def get(self, task_id: str) -> Optional[dict[str, Any]]:
        if await self._use_db():
            return await db_module.task_get(task_id, documents_preview_limit=MAX_DOCS_IN_MEMORY)
        async with self._lock:
            return self._memory.get(task_id)

    async def set_status(
        self,
        task_id: str,
        status: TaskStatus,
        *,
        documents: Optional[list[RAGDocument]] = None,
        error: Optional[str] = None,
    ) -> None:
        if await self._use_db():
            if documents is not None:
                await db_module.documents_save(task_id, [d.model_dump(exclude_none=False) for d in documents])
            await db_module.task_update_status(
                task_id,
                status.value,
                result_count=len(documents) if documents is not None else None,
                error=error,
            )
            return
        async with self._lock:
            if task_id not in self._memory:
                return
            t = self._memory[task_id]
            t["status"] = status
            t["updated_at"] = datetime.utcnow()
            if documents is not None:
                full_count = len(documents)
                self._exports_dir.mkdir(parents=True, exist_ok=True)
                path = self._exports_dir / f"task_{task_id}.ndjson"
                try:
                    export_to_ndjson(documents, path, include_processing_info=True)
                    t["documents_path"] = str(path)
                except Exception:
                    t["documents_path"] = None
                t["result_count"] = full_count
                t["documents"] = [d.model_dump(exclude_none=False) for d in documents[:MAX_DOCS_IN_MEMORY]]
            if error is not None:
                t["error"] = error

    async def run_task(self, task_id: str) -> None:
        task = await self.get(task_id)
        status = task.get("status") if task else None
        if not task or status not in (TaskStatus.pending, TaskStatus.pending.value, "pending"):
            return
        req = task.get("request")
        if isinstance(req, dict):
            request_dict = req
        elif isinstance(req, str):
            request_dict = json.loads(req)
        else:
            request_dict = req.model_dump() if hasattr(req, "model_dump") else dict(req)
        timeout_sec = _get_collection_timeout()
        async with _collection_semaphore:
            await self.set_status(task_id, TaskStatus.running)
            try:
                loop = asyncio.get_event_loop()
                docs_dicts = await asyncio.wait_for(
                    loop.run_in_executor(None, _sync_run_collection, request_dict),
                    timeout=timeout_sec,
                )
                documents = [RAGDocument.model_validate(d) for d in docs_dicts]
                await self.set_status(task_id, TaskStatus.completed, documents=documents)
            except asyncio.TimeoutError:
                print(f"Task {task_id}: collection timeout ({timeout_sec}s), marking failed", flush=True)
                await self.set_status(
                    task_id, TaskStatus.failed,
                    error=f"Collection timeout ({timeout_sec}s)",
                )
            except Exception as e:
                err_msg = (str(e) or repr(e) or type(e).__name__).strip() or "Unknown error"
                print(f"Task {task_id} failed: {err_msg}\n{traceback.format_exc()}", flush=True)
                await self.set_status(task_id, TaskStatus.failed, error=err_msg)


_task_queue: Optional[TaskQueue] = None


def get_task_queue() -> TaskQueue:
    global _task_queue
    if _task_queue is None:
        _task_queue = TaskQueue()
    return _task_queue
