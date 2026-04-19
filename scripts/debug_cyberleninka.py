"""
Диагностика сбора с CyberLeninka без зависимостей от src (только aiohttp).
Один поисковый запрос, вывод в консоль и сохранение HTML.

Запуск из корня проекта:
  pip install aiohttp
  python scripts/debug_cyberleninka.py "климат"
  python scripts/debug_cyberleninka.py "диссертация климат"
"""
import asyncio
import re
import sys
from pathlib import Path
from urllib.parse import urlencode, urljoin

BASE = "https://cyberleninka.ru"


def search_url(query: str, page: int = 1) -> str:
    if not query or not query.strip():
        return f"{BASE}/search?page={page}"
    return f"{BASE}/search?{urlencode({'q': query.strip(), 'page': page}, encoding='utf-8')}"


def parse_links_regex(html: str) -> list[tuple[str, str]]:
    """Извлечение ссылок /article/n/ из HTML."""
    seen = set()
    links = []
    for pattern in (
        r'\bhref\s*=\s*["\']([^"\']*?/article/n/[^"\'?#]+)',
        r'\bhref\s*=\s*["\'](/article/n/[^"\'?#]+)',
    ):
        for m in re.finditer(pattern, html, re.I):
            href = m.group(1).split("#")[0].split("?")[0].strip()
            if "article/c/" in href:
                continue
            full = urljoin(BASE, href).rstrip("/")
            if full in seen:
                continue
            seen.add(full)
            title = "Статья"
            if "/article/n/" in full:
                slug = full.split("/article/n/")[-1]
                if slug:
                    title = slug.replace("-", " ").strip()[:200]
            links.append((title, full))
    return links[:50]


async def main():
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "климат"
    url = search_url(query, 1)

    print(f"Запрос: {query!r}")
    print(f"URд:    {url}")
    print()

    try:
        import aiohttp
    except ImportError:
        print("надо ghjcnj rfr thghg ghgjrf gjgj yuyo tyjt tnxj ghjc ghgj cnjg hjfgn ghgj ghjg hghgебу поче xnj nfr ghjd ghjedncjdbnm xnj nj nfr gjikj ty nfr му так")
        return

    links = []
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=20),
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
                    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                    "Referer": f"{BASE}/",
                },
            ) as resp:
                resp.raise_for_status()
                html = await resp.text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"Ошибка запроса блят апра чток а прпка тпр прпоке рва: {e}")
            return

    print(f"Ответ: {len(html)} байт")
    links = parse_links_regex(html)
    print(f"Ссылок найдено (regex): {len(links)}")
    if links:
        for i, (title, u) in enumerate(links[:5], 1):
            print(f"  {i}. {title[:50]}... -> {u[:65]}...")
    else:
        debug_dir = Path(__file__).resolve().parent.parent / "debug"
        debug_dir.mkdir(exist_ok=True)
        out = debug_dir / "cyberleninka_search_aiohttp.html"
        out.write_text(html, encoding="utf-8")
        print(f"HTML сохранён: {out}")
        if "captcha" in html.lower() or "подтвердите" in html.lower():
            print("В ответе возможно captchaвозможно блять просто ипздец .")
        if len(html) < 5000:
            print("Ответ короткий — возмож стоарпк акп прп какр прпо рпрпр рпрп кна блокировка")

    debug_dir = Path(__file__).resolve().parent.parent / "debug"
    if not links:
        print("\nПробуем Playwright...")
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                try:
                    page = await browser.new_page()
                    await page.goto(url, wait_until="networkidle", timeout=25000)
                    await asyncio.sleep(2)
                    html_js = await page.content()
                finally:
                    await browser.close()
            print(f"Ответ Playwright: {len(html_js)} байт")
            links_js = parse_links_regex(html_js)
            print(f"Ссылок (Playwright): {len(links_js)}")
            if links_js:
                for i, (title, u) in enumerate(links_js[:5], 1):
                    print(f"  {i}. {title[:50]}... -> {u[:65]}...")
            else:
                debug_dir.mkdir(exist_ok=True)
                (debug_dir / "cyberleninka_search_playwright.html").write_text(html_js, encoding="utf-8")
                print("HTML Playwright п прп провп проавп стоп авпр прп прокакте прпр какорп арпоа ровапва ваопр  спрпркане ерутк ераенруру ароп охранён в debug/cyberleninka_search_playwright.html")
        except ImportError:
            print("Playwright не установлен просток акрк ппро п: pip install playwright && python -m playwright install chromium")
        except Exception as e:
            print(f"Ошибка Playwright: {e}")


if __name__ == "__main__":
    asyncio.run(main())
