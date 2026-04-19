# Деплой на Beget cVPS (Docker) — Полная инструкция

## Что вы получите после деплоя

- Веб-интерфейс: `http://212.8.226.190:8000/` — доступен с любого устройства (ПК, телефон, заказчик)
- API документация: `http://212.8.226.190:8000/docs` — Swagger UI
- Фронтенд с тремя режимами: научный поиск, SERP, парсинг URL
- Все результаты сохраняются в PostgreSQL
- Приложение автоматически перезапускается при сбоях

## Реквизиты доступа

| Что | Значение |
|-----|----------|
| SSH сервер | `root@212.8.226.190` |
| Внешний IP | `212.8.226.190` |
| Приватный IP | `10.16.0.1` |
| PostgreSQL хост | `10.16.0.1:5432` |
| PostgreSQL база | `default_db` |
| PostgreSQL логин | `cloud_user` |
| PostgreSQL пароль | `A6*PsdWslI2T` |

---

## Шаг 1. Подготовка на вашем компьютере (Windows)

Откройте **PowerShell** и перейдите в папку проекта:

```powershell
cd C:\kwork\paraser_1state
```

Убедитесь, что файл `.env` существует:

```powershell
Get-Content .env
```

Должно быть:
```
DATABASE_URL=postgresql://cloud_user:A6%2APsdWslI2T@10.16.0.1:5432/default_db
```

Если файла нет — создайте:
```powershell
"DATABASE_URL=postgresql://cloud_user:A6%2APsdWslI2T@10.16.0.1:5432/default_db" | Out-File -Encoding utf8 .env
```

---

## Шаг 2. Загрузка проекта на сервер

### Вариант A: SCP напрямую (рекомендуется)

Из PowerShell на вашем компьютере:

```powershell
# Подключаемся к серверу и создаём папку
ssh root@212.8.226.190 "mkdir -p /opt/parser"

# Загружаем проект (исключаем лишнее)
# Копируем файлы по отдельности, чтобы не тащить venv
scp -r src config static requirements.txt Dockerfile docker-compose.yml .env .dockerignore main.py root@212.8.226.190:/opt/parser/
```

При первом подключении SSH спросит "Are you sure you want to continue connecting?" — введите `yes`.
Введите пароль от сервера (от Beget панели).

### Вариант B: Через архив (если scp -r не работает)

```powershell
# Создаём архив (нужен 7-Zip или встроенный tar)
tar -czf parser.tar.gz --exclude=venv --exclude=__pycache__ --exclude=.git --exclude=debug --exclude=.pytest_cache -C C:\kwork\paraser_1state .

# Загружаем архив
scp parser.tar.gz root@212.8.226.190:/opt/

# Подключаемся и распаковываем
ssh root@212.8.226.190
mkdir -p /opt/parser && cd /opt/parser
tar -xzf /opt/parser.tar.gz
rm /opt/parser.tar.gz
```

---

## Шаг 3. Подключение к серверу по SSH

```powershell
ssh root@212.8.226.190
```

После входа вы окажетесь на Linux-сервере. Все дальнейшие команды выполняются на сервере.

---

## Шаг 4. Проверка файлов на сервере

```bash
cd /opt/parser

# Проверяем, что всё на месте
ls -la
```

Должны быть файлы:
```
.env
.dockerignore
Dockerfile
docker-compose.yml
main.py
requirements.txt
config/          (папка с settings.yaml)
src/             (папка с исходным кодом)
static/          (папка с index.html — фронтенд!)
```

Проверяем `.env`:
```bash
cat .env
```

Если `.env` отсутствует или пуст — создаём:
```bash
cat > /opt/parser/.env << 'EOF'
DATABASE_URL=postgresql://cloud_user:A6%2APsdWslI2T@10.16.0.1:5432/default_db
EOF
```

---

## Шаг 5. Проверка Docker

```bash
# Docker должен быть предустановлен на Beget cVPS
docker --version
docker compose version
```

Если `docker compose` не работает, попробуйте `docker-compose` (с дефисом):
```bash
docker-compose version
```

Если Docker не установлен:
```bash
curl -fsSL https://get.docker.com | sh
systemctl enable docker
systemctl start docker
```

---

## Шаг 6. Сборка Docker-образа

```bash
cd /opt/parser

# Собираем образ (ПЕРВЫЙ РАЗ — 5-15 минут, скачивается Python + Chromium ~1.5 GB)
docker compose build
```

Вы увидите вывод типа:
```
[+] Building 180.5s (12/12) FINISHED
 => [internal] load build definition from Dockerfile
 => => transferring dockerfile: ...
 => CACHED [1/7] FROM python:3.11-slim
 => [2/7] RUN apt-get update && apt-get install -y ...
 => [3/7] COPY requirements.txt .
 => [4/7] RUN pip install --no-cache-dir -r requirements.txt
 => [5/7] RUN playwright install chromium ...
 => [6/7] COPY . .
 => exporting to image
```

Если на этапе `playwright install chromium` ошибка — см. раздел "Устранение проблем".

---

## Шаг 7. Запуск приложения

```bash
cd /opt/parser

# Запускаем в фоновом режиме
docker compose up -d

# Проверяем статус (должно быть "Up" или "running")
docker compose ps
```

Ожидаемый вывод:
```
NAME            IMAGE            COMMAND                  STATUS         PORTS
rag-collector   parser-app       "uvicorn src.app:app…"   Up 10 seconds  0.0.0.0:8000->8000/tcp
```

Смотрим логи запуска:
```bash
docker compose logs -f app
```

Ожидаемые строки:
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete.
Database: connected to PostgreSQL
```

Нажмите `Ctrl+C` чтобы выйти из просмотра логов (приложение продолжит работать).

---

## Шаг 8. Открытие порта в файрволе

```bash
# Проверяем файрвол
ufw status

# Если файрвол активен — открываем порт 8000
ufw allow 8000/tcp
ufw reload

# Если файрвол не активен (inactive) — порт уже доступен
```

---

## Шаг 9. Проверка работоспособности

### С сервера:
```bash
# Проверяем, что API отвечает
curl http://localhost:8000/health

# Ожидаемый ответ:
# {"status":"ok"}
```

### С вашего компьютера (в браузере):

1. Откройте: **http://212.8.226.190:8000/**
   - Должен загрузиться веб-интерфейс "RAG Data Collector"

2. Откройте: **http://212.8.226.190:8000/docs**
   - Должна загрузиться Swagger документация API

3. На вкладке **"Научные источники"** нажмите **"Собрать данные"**
   - Через 30-60 секунд должны появиться результаты

### С телефона / другого устройства:

Откройте в браузере: `http://212.8.226.190:8000/`

---

## Управление приложением

### Просмотр логов
```bash
# В реальном времени
docker compose logs -f app

# Последние 100 строк
docker compose logs --tail=100 app
```

### Остановка
```bash
docker compose down
```

### Перезапуск
```bash
docker compose restart
```

### Пересборка после изменений кода
```bash
docker compose up -d --build
```

### Зайти внутрь контейнера
```bash
docker compose exec app bash
```

---

## Обновление проекта

На вашем компьютере (PowerShell):
```powershell
# Загружаем обновлённые файлы
scp -r src config static requirements.txt Dockerfile docker-compose.yml main.py root@212.8.226.190:/opt/parser/
```

На сервере:
```bash
cd /opt/parser
docker compose up -d --build
```

---

## Устранение проблем

### Приложение не запускается

```bash
# Смотрим логи
docker compose logs app

# Перестроить образ с нуля
docker compose down
docker compose build --no-cache
docker compose up -d
```

### "Database: connection failed"

```bash
# Проверяем подключение к PostgreSQL из контейнера
docker compose exec app python -c "
import asyncio, asyncpg
async def test():
    conn = await asyncpg.connect('postgresql://cloud_user:A6%2APsdWslI2T@10.16.0.1:5432/default_db')
    print('OK! Version:', conn.get_server_version())
    await conn.close()
asyncio.run(test())
"
```

Если ошибка "connection refused":
- Проверьте что PostgreSQL запущен в панели Beget
- Попробуйте внешний хост вместо приватного IP:
```bash
# Меняем .env
cat > /opt/parser/.env << 'EOF'
DATABASE_URL=postgresql://cloud_user:A6%2APsdWslI2T@ladidategip.beget.app:5432/default_db
EOF
docker compose restart
```

### Порт 8000 не открывается снаружи

```bash
# Проверяем, слушает ли контейнер
docker compose ps
# Должен быть: 0.0.0.0:8000->8000/tcp

# Проверяем, открыт ли порт
ss -tlnp | grep 8000

# Если Beget блокирует порт — используйте порт 80:
# В docker-compose.yml меняем: "80:8000" вместо "8000:8000"
```

### CyberLeninka не работает (CAPTCHA / Playwright)

```bash
# Проверяем Chromium
docker compose exec app playwright install --dry-run

# Если не установлен:
docker compose exec app playwright install chromium
docker compose exec app playwright install-deps chromium
docker compose restart
```

### Мало памяти / контейнер убивается

```bash
# Проверяем память
free -h

# Если мало RAM — ограничиваем Docker
# В docker-compose.yml добавить:
# deploy:
#   resources:
#     limits:
#       memory: 2G
```

### Очистка дискового пространства

```bash
# Удаляем неиспользуемые Docker-образы
docker system prune -f

# Удаляем все остановленные контейнеры и образы
docker system prune -a -f
```

---

## Структура на сервере

```
/opt/parser/
  .env                  # DATABASE_URL (секреты, НЕ коммитить в git)
  .dockerignore         # Исключения для Docker-образа
  Dockerfile            # Рецепт сборки образа
  docker-compose.yml    # Конфигурация запуска
  main.py               # Точка входа (для локальной разработки)
  requirements.txt      # Python-зависимости
  config/
    settings.yaml       # Настройки приложения (источники, лимиты)
  src/                  # Исходный код Python
    app.py              # FastAPI приложение
    collector.py        # Оркестратор сбора
    sources/            # Адаптеры источников
    ...
  static/
    index.html          # Веб-интерфейс (фронтенд)
```

---

## Быстрый чеклист деплоя

1. [ ] `ssh root@212.8.226.190` — подключились к серверу
2. [ ] `ls /opt/parser/` — файлы проекта на месте
3. [ ] `cat /opt/parser/.env` — DATABASE_URL настроен
4. [ ] `cd /opt/parser && docker compose build` — образ собран
5. [ ] `docker compose up -d` — контейнер запущен
6. [ ] `docker compose ps` — статус "Up"
7. [ ] `curl http://localhost:8000/health` — API отвечает `{"status":"ok"}`
8. [ ] Браузер: `http://212.8.226.190:8000/` — фронтенд загрузился
9. [ ] Нажали "Собрать данные" — результаты появились
