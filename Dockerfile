FROM python:3.11-slim

WORKDIR /app

# Не буферизовать stdout/stderr — логи в реальном времени
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Системные зависимости для pdfminer, Playwright Chromium и общих нужд
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Для PDF-обработки
    libmupdf-dev \
    # Общие зависимости для headless Chromium (Playwright)
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libwayland-client0 \
    # Для скачивания и сетевых операций
    wget \
    ca-certificates \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Зависимости Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Установка браузера Chromium для Playwright (CyberLeninka, CAPTCHA)
RUN playwright install chromium && playwright install-deps chromium

# Копируем исходный код
COPY . .

EXPOSE 8000

CMD ["uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8000"]
