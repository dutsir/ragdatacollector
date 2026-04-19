# Инструкция по тестированию запроса

## Вариант 1: Тест через Python скрипт (рекомендуется)

Запустите тестовый скрипт напрямую:

```bash
python test_request.py
```

Этот скрипт:
- Создаёт запрос со всеми источниками
- Запускает сбор документов
- Выводит статистику по источникам и документам
- Показывает ошибки, если они есть

## Вариант 2: Тест через API (если сервер запущен)

### Windows PowerShell:
```powershell
.\test_request.ps1
```

### Linux/Mac:
```bash
chmod +x test_request.sh
./test_request.sh
```

### Или через curl напрямую:
```bash
curl -X POST "http://localhost:8000/api/v1/collect" \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "science",
    "keywords": ["thesis", "climate change"],
    "date_from": "2020-01-01",
    "date_to": "2026-02-01",
    "languages": ["ru", "en"],
    "sources": ["crossref", "cyberleninka", "openalex", "pubmed", "arxiv", "cinii"],
    "max_results": 3
  }'
```

После отправки запроса вы получите `task_id`. Проверьте статус:

```bash
curl "http://localhost:8000/api/v1/tasks/{task_id}"
```

## Что проверить:

1. **CrossRef** — должен вернуть документы (проверьте, что нет HTML в чанках)
2. **CyberLeninka** — должен вернуть документы
3. **OpenAlex** — должен вернуть документы
4. **PubMed** — может быть timeout (проблема сети), но должен попытаться подключиться
5. **arXiv** — должен вернуть результаты (исправлен формат запроса)
6. **CiNii** — должен вернуть результаты (исправлен парсинг ответа)

## Ожидаемые результаты:

- **CrossRef**: ~3-15 документов (в зависимости от фильтров)
- **CyberLeninka**: ~3-15 документов
- **OpenAlex**: ~2-15 документов
- **PubMed**: 0-9 документов (может быть timeout)
- **arXiv**: ~3-15 документов (теперь должен работать)
- **CiNii**: 0-15 документов (может быть 0 для английского запроса)

**Итого**: ожидается ~8-20 документов после дедупликации и фильтрации.

## Если что-то не работает:

1. Проверьте логи в консоли — там будут `[collector]`, `[crossref]`, `[arxiv]` и т.д.
2. Убедитесь, что все источники зарегистрированы: `python -c "from src.sources import list_sources; print(list_sources())"`
3. Проверьте конфигурацию в `config/settings.yaml`
