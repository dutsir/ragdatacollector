# RAG Data Collector

Система сбора данных из открытых источников для RAG-пайплайнов (Retrieval-Augmented Generation).  
REST API на FastAPI, модульная архитектура, асинхронная обработка, экспорт в NDJSON.

## Требования

- Python 3.10+
- Конфигурация: `config/settings.yaml`

## Установка

```bash
pip install -r requirements.txt
# Если поиск по CyberLeninka возвращает 0 результатов (страница подгружает результаты по JS):
python -m playwright install chromium
```

**Windows:** в `requirements.txt` убраны spacy и fasttext — они тянут за собой blis и при установке требуют сборки C-кода, что часто падает. Для чанкования используется tiktoken, для определения языка — langdetect. PDF (PyMuPDF, pdfplumber) закомментированы; при необходимости ставить отдельно, предпочтительно бинарные колёса.

## Запуск

```bash
# Из корня проекта
python main.py
# или
uvicorn src.app:app --reload --host 0.0.0.0 --port 8000
```

Документация API: http://localhost:8000/docs

**Нагрузка на ноутбук:** сбор с CyberLeninka запускает браузер (Playwright) и загружает много страниц. Одновременно выполняется только одна такая задача; полные результаты пишутся в файл или в БД, в памяти хранится только превью (до 10 документов). Если ноутбук тормозит или закрывает приложения — уменьшите `max_results` (например до 5) или не запускайте другие тяжёлые программы во время сбора.

## База данных (PostgreSQL)

Если задан URL подключения, задачи и документы сохраняются в PostgreSQL (иначе — только in-memory и файлы NDJSON).

1. Создайте БД, например: `CREATE DATABASE parser_1state;`
2. Укажите URL в `config/settings.yaml` в секции `database.url` или задайте переменную окружения:
   ```bash
   set DATABASE_URL=postgresql://postgres:ВАШ_ПАРОЛЬ@localhost:5432/parser_1state
   ```
3. При первом запуске приложения таблицы `tasks` и `documents` создадутся автоматически.

Таблицы: **tasks** (id, status, request, result_count, error, …), **documents** (task_id, title, url, abstract, full_text_chunks, …). Скачивание NDJSON по завершённой задаче отдаёт данные из БД или из файла.

## Docker

```bash
docker build -t rag-data-collector .
docker run -p 8000:8000 rag-data-collector
```

## Конфигурация

Файл `config/settings.yaml`:

- **app** — задержки между запросами (2–5 сек), debug
- **sources** — включённые источники и параметры (base_url, timeout, retries)
- **text_processing** — размер чанков (500–2000 токенов), перекрытие, токенизатор (tiktoken/spacy)
- **language_detection** — провайдер (langdetect/fasttext), список языков
- **validation** — минимальные длины, поля для дедупликации
- **export** — каталог NDJSON, включать ли processing_info

## API

### POST /api/v1/collect

Создать задачу сбора. Тело запроса:

```json
{
  "task_type": "science",
  "keywords": ["геополитика США"],
  "date_from": "2024-01-01",
  "date_to": "2026-02-01",
  "languages": ["ru", "en"],
  "sources": ["cyberleninka"],
  "max_results": 50
}
```

Для парсинга конкретных URL:

```json
{
  "task_type": "target_site",
  "keywords": [],
  "urls": ["https://example.com/article"],
  "sources": ["cyberleninka"]
}
```

Ответ: `{ "task_id": "uuid", "status": "pending", "message": "Task created" }`.

### GET /api/v1/tasks/{task_id}

Статус задачи и при `status=completed` — список документов (JSON).

### GET /api/v1/tasks/{task_id}/ndjson

Скачать результаты в формате NDJSON (один JSON-объект на строку).

### GET /api/v1/sources

Список зарегистрированных источников.

### POST /api/v1/query/suggest

Подсказки по улучшению запроса (тело — объект с полями keywords, languages, date_from, date_to, sources).

## Источники (модули)

- **cyberleninka** — поиск и парсинг статей CyberLeninka (rule-based, BeautifulSoup + aiohttp / Playwright).
- **crossref** — поиск по CrossRef REST API (публичный, без ключа): метаданные, аннотации, DOI; при наличии ссылки на PDF — извлечение текста. Требования как у CyberLeninka (чанки, язык, валидация).

Добавление нового источника:

1. Реализовать класс, наследующий `src.sources.base.BaseSource` (методы `search`, `fetch_article`).
2. Зарегистрировать: `register_source("name", MySource)` в `src.sources.registry` или при импорте модуля.

## Обработка текста

- **Язык**: `langdetect` (поддержка RTL и CJK через коды ru, en, zh, ja, ko, ar, he, yi и др.).
- **Чанкование**: 500–2000 токенов (tiktoken), с перекрытием; при отсутствии tiktoken — по словам.
- **Валидация**: оценка 0–1 по наличию title, url, abstract, чанков; дедупликация по хешу (title, url, abstract).

## Выходной формат документа (RAG)

Каждый документ в ответе и в NDJSON:

- `id`, `title`, `authors`, `date`, `doi`, `url`, `language`, `source`
- `abstract`, `full_text_chunks` (массив строк)
- `files`: `[{ "type": "PDF", "url": "...", "extracted_text": "..." }]`
- `metadata`, `processing_info` (extraction_method, chunking_strategy, validation_score)

## Тесты

```bash
pytest tests/ -v
```

## Лицензия

MIT.
