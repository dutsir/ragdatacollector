"""FastAPI приложение: REST API для сбора данных."""
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from .models.api import (
    CollectRequest,
    CollectResponse,
    TaskStatus,
    TaskStatusResponse,
    TaskInfo,
)
from .tasks import get_task_queue
from .export_ndjson import export_to_ndjson
from .query import UniversalQueryOptimizer, QueryAssistant
from . import db as db_module


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Старт: подключение к PostgreSQL, сброс зависших running. Стоп: закрытие пула."""
    if db_module.DATABASE_URL:
        try:
            ok = await db_module.init_db()
            if ok:
                print("Database: connected to PostgreSQL", flush=True)
                # Задачи в running, не обновлявшиеся 5+ минут — зомби после убитого процесса
                n = await db_module.tasks_mark_stale_running_failed(None)  # все running при старте — зомби
                if n:
                    print(f"Database: marked {n} stale 'running' task(s) as failed", flush=True)
            else:
                print("Database: connection failed, using in-memory storage", flush=True)
        except Exception as e:
            print(f"Database: error - {e}", flush=True)
    else:
        print("Database: no URL configured (DATABASE_URL or config/settings.yaml database.url)", flush=True)
    yield
    await db_module.close_db()


def create_app() -> FastAPI:
    app = FastAPI(
        title="RAG Data Collector",
        description="Сбор данных из открытых источников для RAG-пайплайнов",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS — разрешаем запросы с фронтенда
    from fastapi.middleware.cors import CORSMiddleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    queue = get_task_queue()

    @app.post("/api/v1/collect", response_model=CollectResponse)
    async def create_collect_task(request: CollectRequest):
        """Создать задачу сбора. Задача выполняется в фоне, ответ возвращается сразу."""
        create_timeout_sec = 15
        try:
            task_id = await asyncio.wait_for(queue.create(request), timeout=create_timeout_sec)
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=503,
                detail=f"Task creation timeout ({create_timeout_sec}s). Check database or try again.",
            )
        # Запуск сбора в фоне после отправки ответа (call_soon — следующий тик цикла)
        loop = asyncio.get_event_loop()
        loop.call_soon(lambda: asyncio.create_task(queue.run_task(task_id)))
        return CollectResponse(
            task_id=task_id,
            status=TaskStatus.pending,
            message="Task created",
        )

    @app.get("/api/v1/tasks/{task_id}", response_model=TaskStatusResponse)
    async def get_task_status(task_id: str):
        """Получить статус и результаты задачи."""
        task = await queue.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return TaskStatusResponse(
            task_id=task["task_id"],
            status=task["status"],
            result_count=task.get("result_count", 0),
            error=task.get("error"),
            documents=task.get("documents"),
        )

    @app.get("/api/v1/tasks/{task_id}/ndjson")
    async def download_ndjson(task_id: str):
        """Скачать результаты задачи в формате NDJSON (из файла или из БД)."""
        task = await queue.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        if task["status"] not in (TaskStatus.completed, "completed"):
            raise HTTPException(
                status_code=400,
                detail=f"Task not completed (status: {task['status']})",
            )
        path_str = task.get("documents_path")
        if path_str:
            path = Path(path_str)
            if path.exists():
                return FileResponse(
                    path,
                    media_type="application/x-ndjson",
                    filename=f"collect_{task_id}.ndjson",
                )
        from . import db as db_module
        if await db_module.get_pool():
            docs = await db_module.documents_get_by_task_id(task_id, limit=None)
            if docs:
                import json
                lines = [json.dumps(d, ensure_ascii=False) + "\n" for d in docs]
                from fastapi.responses import Response
                return Response(
                    content="".join(lines),
                    media_type="application/x-ndjson",
                    headers={"Content-Disposition": f"attachment; filename=collect_{task_id}.ndjson"},
                )
        docs = task.get("documents")
        if not docs and task.get("result_count", 0) > 0:
            return PlainTextResponse(
                "# Documents in DB or file not found.",
                media_type="text/plain",
            )
        if not docs:
            return PlainTextResponse("", media_type="application/x-ndjson")
        from .models.document import RAGDocument
        rag_docs = [RAGDocument.model_validate(d) for d in docs]
        export_dir = Path("./exports")
        export_dir.mkdir(parents=True, exist_ok=True)
        path = export_dir / f"task_{task_id}.ndjson"
        export_to_ndjson(rag_docs, path, include_processing_info=True)
        return FileResponse(
            path,
            media_type="application/x-ndjson",
            filename=f"collect_{task_id}.ndjson",
        )

    @app.get("/api/v1/sources", response_model=list[str])
    async def list_sources():
        """Список доступных источников."""
        from .sources import list_sources as get_sources
        return get_sources()

    @app.post("/api/v1/query/suggest")
    async def suggest_query_improvements(body: dict):
        """Подсказки по улучшению запроса."""
        assistant = QueryAssistant()
        return assistant.suggest_improvements(body)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    # --- Frontend ---
    static_dir = Path(__file__).resolve().parent.parent / "static"
    if static_dir.is_dir():
        @app.get("/", response_class=HTMLResponse)
        async def frontend():
            """Отдать главную страницу фронтенда."""
            index_path = static_dir / "index.html"
            if index_path.exists():
                return HTMLResponse(content=index_path.read_text(encoding="utf-8"))
            return HTMLResponse("<h1>index.html not found</h1>", status_code=404)

        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app


app = create_app()
