# Трек-лист и дорожная карта: соответствие ТЗ и масштабирование

Документ описывает: **что реализовано**, **что улучшить**, **как подключать новые источники** для задач 1–3 из ТЗ.

---

## 1. Что получилось сделать (итог)

### 1.1 Общие требования ТЗ

| Требование | Статус | Комментарий |
|------------|--------|-------------|
| Только открытые источники, без обхода ToS | ✅ | Задержки, User-Agent, без логинов |
| Мультиязычность (ru, en, fr, de, es, zh, ja, ko, ar, he, yi) | ✅ | Коды в API, langdetect, фильтр по `languages` |
| Задержки между запросами | ✅ | `config/settings.yaml` по источникам |
| Вход: ключевые слова, даты, URL (опц.), языки | ✅ | `CollectRequest`: keywords, date_from/date_to, urls, languages |
| Выход: JSON/NDJSON, метаданные, чанки 500–2000 токенов | ✅ | `RAGDocument`, NDJSON-экспорт, чанки из конфига |

### 1.2 Выходная структура (по ТЗ)

Поля из примера ТЗ реализованы в `RAGDocument`:

| Поле ТЗ | Поле в коде | Статус |
|---------|--------------|--------|
| id | id | ✅ |
| title | title | ✅ |
| authors | authors | ✅ |
| date | date | ✅ |
| doi/url | doi, url | ✅ |
| abstract | abstract | ✅ |
| full_text_chunks[] | full_text_chunks | ✅ |
| language | language | ✅ |
| source | source | ✅ |
| files (PDF, url, extracted_text) | files | ✅ |
| — | relevance_score, file_access, crawling_timestamp | ✅ Доп. поля для RAG |

### 1.3 Задача 1: Наукометрические базы и авторефераты

| Источник | Статус | Как подключён |
|----------|--------|----------------|
| **CyberLeninka** | ✅ | Поиск (Playwright/aiohttp), парсинг статей, PDF, чанки, `sources.registry` |
| **CrossRef** | ✅ | REST API, метаданные, DOI, аннотация, PDF/Unpaywall, link (text/plain, text/xml), фильтр по языку и релевантности |
| **OpenAlex** | ✅ | REST API, search по title/abstract, даты, язык, abstract_inverted_index→текст, best_oa_location (PDF), fetch по OpenAlex ID/DOI |
| **PubMed** | ✅ | NCBI E-utilities (esearch + efetch), даты (mindate/maxdate), язык по детекции, PMID/DOI/PMC, ссылка на PDF для PMC, fetch по URL/PMID |
| **arXiv** | ✅ | REST API (Atom), search_query all:...+submittedDate, даты, PDF-ссылка, DOI (arxiv:doi), fetch по arxiv.org/abs/ID |
| **CiNii Dissertations** | ✅ | OpenSearch (format=json), year_from/year_to, JSON-LD разбор, ссылки на полные тексты (dc:source), fetch по ci.nii.ac.jp/d/NAID или /naid/NAID.json |
| РГБ (ldiss.rsl.ru) | ❌ | Не реализован |
| dslib.net | ❌ | Не реализован |
| OATD | ❌ | Не реализован |
| DART-Europe | ❌ | Не реализован |

### 1.4 Задача 2: Общий поиск по интернету (SERP)

| Требование | Статус |
|------------|--------|
| Локализованный SERP, топ 10–50 | ✅ DuckDuckGo HTML SERP (`google_serp`), с локализацией, snippet |
| snippet, relevance_score | ✅ snippet в metadata и abstract; relevance_score в модели |

### 1.5 Задача 3: Сбор с конкретных сайтов по URL

| Требование | Статус |
|------------|--------|
| Указание URL, извлечение текста/PDF | ✅ `task_type=target_site`, `urls` в запросе |
| Реализация | ✅ Работает для зарегистрированных источников + fallback через `universal_url` (HTML/PDF парсер произвольных URL) |

### 1.6 Формат запроса (несколько источников, лимит на каждый)

- **`sources`** — список источников. Один и тот же запрос (keywords, date_from, date_to, languages) применяется ко **всем** указанным источникам.
- **`max_results`** — лимит результатов **с каждого** источника (итого до `len(sources) * max_results` документов после дедупликации).
- Допускается передача в одном элементе через запятую: `"sources": ["crossref", "cyberleninka"]` или `"sources": ["crossref, cyberleninka"]` — оба варианта нормализуются в список `["crossref", "cyberleninka"]`.

**Пример запроса:**
```json
{
  "task_type": "science",
  "keywords": ["USA"],
  "date_from": "2024-01-01",
  "date_to": "2026-02-01",
  "languages": ["ru", "en"],
  "sources": ["crossref", "cyberleninka", "openalex"],
  "max_results": 5
}
```
С каждого из трёх источников будет запрошено до 5 результатов; общие параметры (keywords, даты, языки) — одни и те же.

### 1.7 Дополнительно реализовано

- Фильтрация по языку (метаданные API + langdetect), исключение неподдерживаемых кодов.
- Расчёт релевантности по ключевым словам, порог и fallback (топ по score при 0 проходов).
- Синонимы для запросов (USA ↔ United States, СШA и т.д.).
- Извлечение полных текстов из CrossRef (link: PDF, text/plain, text/xml), учёт rate limit.
- Закрытие HTTP-сессий (CrossRef), подавление предупреждений PyMuPDF при разборе PDF.
- PostgreSQL для задач и документов, экспорт NDJSON.

---

## 2. Трек-лист улучшений (по приоритету)

### Высокий приоритет (для соответствия ТЗ и тестам)

- [ ] **Задача 2 — SERP:** Реализовать источник `google_serp` (или аналог). Варианты: SerpAPI, Bing Web Search API, парсинг с соблюдением ToS. Зарегистрировать в `sources.registry`, вызывать при `task_type=web_search` из `collector`.
- [ ] **Задача 3 — Универсальный парсер URL:** Реализовать источник «generic» или «target_site» с методом `fetch_article(url)` для произвольного URL: загрузка HTML (aiohttp/Playwright), извлечение текста (BeautifulSoup/readability), опционально PDF по ссылкам. Поддержать в collector для `target_site` при `sources: ["generic"]` или отдельном флаге.
- [ ] **Задача 1 — Новые базы:** Подключить по одному: OpenAlex, arXiv, PubMed (REST API), затем по приоритету: РГБ, dslib.net, OATD, DART-Europe, CiNii. Для каждого: класс-источник, `search()` + при необходимости `fetch_article()`, `to_rag_document()`, регистрация в registry.

### Средний приоритет (качество и устойчивость)

- [ ] Ротация User-Agent и опционально прокси (конфиг, без нарушения ToS).
- [ ] В SERP/общем поиске: поле `snippet` в метаданных или в модели.
- [ ] Для target_site: фильтрация по ключевым словам в теле страницы и по дате публикации (если извлекается из страницы).
- [ ] Расширить тесты: Тест 1 (диссертация климат), Тест 2 (экономика РФ 2025, SERP), Тест 3 (example.com или выбранный сайт).

### Низкий приоритет

- [ ] Экспорт в CSV (сейчас JSON/NDJSON).
- [ ] Конфигурируемый список синонимов запросов в `settings.yaml`.
- [ ] Документация по запуску и конфигу (актуализировать README под текущие источники и task_type).

---

## 3. Как подключить новый парсинг (масштабирование)

Архитектура уже рассчитана на добавление источников без переписывания ядра.

### 3.1 Добавление источника для Задачи 1 (наукометрика / диссертации)

1. **Создать класс в `src/sources/`** (например, `openalex.py`), наследовать `BaseSource` из `src/sources/base.py`.
2. Реализовать:
   - `name: str`
   - `async def search(query, max_results=..., date_from=..., date_to=..., languages=...) -> list[SourceResult]`
   - при необходимости: `async def fetch_article(url) -> SourceResult | None`
   - `def to_rag_document(result: SourceResult, **kwargs) -> RAGDocument`
3. В конце файла зарегистрировать: `register_source("openalex", OpenAlexSource)`.
4. В `config/settings.yaml` в `sources.enabled` добавить `openalex` и при необходимости секцию `sources.openalex` (url, задержки, ключи API).
5. Коллектор уже вызывает `get_source(name)` и для каждого имени из `request.sources` вызывает `search()` и `to_rag_document()`; дедупликация и фильтр по дате общие.

Типовой скелет:

```python
# src/sources/openalex.py
from .base import BaseSource, SourceResult
from .registry import register_source
from ..models.document import RAGDocument

class OpenAlexSource(BaseSource):
    name = "openalex"

    async def search(self, query, *, max_results=50, date_from=None, date_to=None, languages=None):
        # Запрос к API, формирование list[SourceResult]
        return results

    async def fetch_article(self, url: str):
        # Опционально: по URL/DOI вернуть один SourceResult
        return None

    def to_rag_document(self, result: SourceResult, **kwargs) -> RAGDocument:
        # Единый формат: id, title, authors, date, doi, url, language, source, abstract, full_text_chunks, ...
        return doc

register_source("openalex", OpenAlexSource)
```

### 3.2 Подключение Задачи 2 (SERP / общий поиск)

1. Реализовать источник, например `google_serp.py` (или SerpAPI-обёртку).
2. В `search()`: вызов внешнего API поиска или парсинг выдачи; возвращать `list[SourceResult]` с полями хотя бы title, url, abstract (snippet).
3. В `to_rag_document()` заполнять `relevance_score` и при необходимости `metadata["snippet"]`.
4. В `collector.py` для `task_type=web_search` вызывать этот источник (сейчас тип есть в API, но отдельной ветки в collector может не быть — добавить ветку `if request.task_type == "web_search"` и итерацию по источникам, как для science).
5. Добавить источник в `sources.enabled` и в конфиг (API-ключ, лимиты).

### 3.3 Подключение Задачи 3 (целевые сайты по URL)

1. **Вариант A — универсальный парсер:** Новый источник, например `generic_site.py`, с методом `fetch_article(url)`:
   - загрузка HTML (aiohttp или Playwright при необходимости);
   - извлечение основного текста (BeautifulSoup, readability-lxml и т.п.);
   - опционально: поиск ссылок на PDF на странице и извлечение текста из PDF;
   - возврат `SourceResult(title=..., url=..., abstract=..., full_text=...)`.
2. В collector для `target_site` уже есть цикл по `request.urls` и вызов `source.fetch_article(url)` по каждому источнику из `request.sources`. Достаточно добавить в `sources` имя универсального источника (например `generic`) и вызывать его для произвольных URL.
3. **Вариант B — парсер под конкретный сайт:** Отдельный класс под домен (как CyberLeninka), со своей логикой извлечения (селекторы, дата, разделы). Регистрация в registry и указание в `sources` при запросе с `task_type=target_site`.

### 3.4 Единый контракт (для любого нового источника)

- **Вход:** те же параметры, что в `CollectRequest`: keywords, date_from, date_to, languages, max_results; для target_site — urls.
- **Выход:** список `RAGDocument` с полями по ТЗ (id, title, authors, date, doi/url, abstract, full_text_chunks, language, source, files; при необходимости relevance_score, file_access, crawling_timestamp).
- Чанкование и определение языка можно оставить в общем пайплайне (в `to_rag_document` каждого источника вызываются общие функции из `processing`), чтобы не дублировать логику.

---

## 4. Соответствие тестовым сценариям ТЗ

| Тест | Текущее состояние | Что сделать |
|------|-------------------|-------------|
| **Тест 1:** 10 авторефератов «диссертация климат» (ru/en), структура и чанки | Частично: можно набрать статьи по CyberLeninka + CrossRef; авторефераты и диссертационные базы — только после подключения РГБ/dslib/OATD и т.д. | Подключить минимум один источник диссертаций; добавить автотест на структуру и чанки. |
| **Тест 2:** Поиск «экономика РФ 2025», top-20, 3 языка | Невозможен без SERP | Реализовать источник SERP (Задача 2). |
| **Тест 3:** Сайт example.com — извлечь статьи/PDF | Только для источников с `fetch_article` (CyberLeninka, CrossRef по DOI) | Реализовать универсальный парсер по URL (Задача 3). |

---

## 5. Краткая схема: куда что подключать

```
CollectRequest (keywords, dates, languages, sources, urls, task_type)
       │
       ▼
collector.run_collection()
       │
       ├── task_type == "target_site" && urls
       │      → для каждого source из request.sources: source.fetch_article(url) → to_rag_document
       │
       ├── task_type == "web_search"   [пока не реализована ветка]
       │      → для каждого source: source.search(...) → to_rag_document
       │
       └── task_type == "science" (по умолчанию)
              → для каждого source: source.search(...) → to_rag_document
       │
       ▼
Дедупликация по content_hash, фильтр по дате
       ▼
list[RAGDocument] → сохранение в БД / NDJSON / ответ API
```

Новый парсинг подключается как новый источник в `sources/` + запись в `sources.enabled` и при необходимости отдельная ветка под `task_type` в `collector.py`.

---

## 6. Итог

- **Сделано:** два полноценных источника (CyberLeninka, CrossRef), единая модель RAGDocument, API, очередь задач, БД, фильтры по языку и релевантности, полные тексты из CrossRef, этичный режим (задержки, закрытие сессий).
- **Чтобы закрыть ТЗ по задачам 1–3:** добавить источник SERP (Задача 2), универсальный парсер по URL (Задача 3), затем по приоритету — OpenAlex, arXiv, PubMed, РГБ/dslib/OATD и др. (Задача 1).
- **Масштабирование:** все новые парсеры подключаются через новый класс-источник и `register_source()`; коллектор и выходная структура уже готовы.

Документация по запуску и конфигу — в `README.md`; соответствие ТЗ по пунктам — в `SPEC_COMPLIANCE.md`.
