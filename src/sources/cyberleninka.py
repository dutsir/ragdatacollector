"""Пилотный источник: CyberLeninka (наукометрическая база). Rule-based парсинг."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional
import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urljoin, urlencode

import aiohttp
from bs4 import BeautifulSoup

from ..models.document import FileRef, ProcessingInfo, RAGDocument
from ..processing import (
    chunk_text,
    compute_chunk_info,
    detect_language,
    compute_validation_score,
    document_type_from_text,
    extract_metadata_from_pdf_text,
    filter_and_clean_chunks,
)
from ..processing.validation import (
    ensure_chunks_end_at_boundaries,
    dedupe_chunks_by_hash,
    normalize_date,
    validate_text_ends_complete,
)
from ..processing.pdf_extract import extract_text_from_pdf_url
from ..processing.doi_resolve import fetch_doi_from_crossref
from .base import BaseSource, SourceResult, SourceTemporarilyUnavailableError
from .registry import register_source

logger = logging.getLogger(__name__)

# Регулярки для извлечения ссылок на статьи (в том числе и относительнеы относительные и с query-параметрами)
ARTICLE_LINK_RE = re.compile(
    r'\bhref\s*=\s*["\']([^"\']*?/article/n/[^"\'?#]+)',
    re.I,
)
ARTICLE_LINK_ALT_RE = re.compile(
    r'\bhref\s*=\s*["\']([^"\']*?/article/(?!c/)[^"\'?#]+)',
    re.I,
)
# Относительные пути вида href="/article/n/..."
ARTICLE_REL_RE = re.compile(
    r'\bhref\s*=\s*["\'](/article/n/[^"\'?#]+)',
    re.I,
)
# Любое вхождение /article/n/... в HTML (JSON, data-атрибуты, минифицированный JS)
ARTICLE_ANY_RE = re.compile(
    r'(?:href\s*=\s*["\']?|["\']|/)(article/n/[a-zA-Z0-9_.-]+)',
    re.I,
)

# Запасные URL категорий, если главная отдаёт только JS-оболочку (поиск через aiohttp без Playwright)
FALLBACK_CATEGORY_PATHS = [
    "article/c/earth-and-related-environmental-sciences",
    "article/c/computer-and-information-sciences",
    "article/c/economics-and-business",
    "article/c/educational-sciences",
    "article/c/psychology",
]

# User-Agent по умолчанию (реальный Chrome), можно переопределить в config/settings.yaml
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Прямые URL статей для проверки, когда обход по категориям даёт 0 (страницы категорий тоже могут быть JS)
FALLBACK_ARTICLE_PATHS = [
    "article/n/vozmozhnye-puti-razvitiya-otkrytoy-nauki-v-rossii",
    "article/n/otkrytyy-dostup-k-nauke-mify-i-realnost",
    "article/n/infrastruktura-otkrytoy-nauki",
]


def _debug_log(msg: str) -> None:
    """Пишет строку в debug/collection_log.txt (видны этапы сбора при result_count=0)."""
    try:
        from datetime import datetime
        debug_dir = Path("debug")
        debug_dir.mkdir(exist_ok=True)
        log_path = debug_dir / "collection_log.txt"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} | {msg}\n")
    except Exception:
        pass


def _is_captcha_page(html: str) -> bool:
    """Проверка, что в HTML страница CAPTCHA КиберЛенинки."""
    if not html or len(html) < 200:
        return False
    lower = html.lower()
    return (
        "вы точно человек?" in lower
        or "g-recaptcha" in html
        or "для продолжения работы вам необходимо ввести капчу" in lower
    )


async def _try_solve_recaptcha_and_submit(page, headless: bool = True) -> bool:
    """
    Пытается решить reCAPTCHA v2: либо через playwright-recaptcha (аудио), либо при headless=False
    ждёт, пока пользователь решит капчу вручную (до 120 сек).
    """
    try:
        from playwright_recaptcha import recaptchav2
    except ImportError:
        if not headless:
            print("[cyberleninka] Решите CAPTCHA вручную в открывшемся окне (ждём до 120 сек)...", flush=True)
            try:
                await page.wait_for_selector("div.ocr, div.main", timeout=120000)
                return True
            except Exception:
                return False
        return False
    solver_cls = getattr(recaptchav2, "AsyncSolver", None)
    if not solver_cls:
        if not headless:
            print("[cyberleninka] AsyncSolver не найден. Решите CAPTCHA вручную (ждём до 120 сек)...", flush=True)
            try:
                await page.wait_for_selector("div.ocr, div.main", timeout=120000)
                return True
            except Exception:
                return False
        return False
    try:
        async with solver_cls(page) as solver:
            token = await solver.solve_recaptcha(wait=True)
        if not token:
            if not headless:
                print("[cyberleninka] Солвер не вернул токен. Решите CAPTCHA вручную (ждём до 120 сек)...", flush=True)
                try:
                    await page.wait_for_selector("div.ocr, div.main", timeout=120000)
                    return True
                except Exception:
                    return False
            return False
        await page.evaluate(
            """(token) => {
            const field = document.querySelector('textarea[name="g-recaptcha-response"]');
            if (field) field.value = token;
            else {
                const el = document.createElement('textarea');
                el.name = 'g-recaptcha-response';
                el.style.display = 'none';
                el.value = token;
                document.body.appendChild(el);
            }
        }""",
            token,
        )
        submit = await page.query_selector('input.btn[type="submit"], input[type="submit"][value*="родолжить"], input[type="submit"]')
        if submit:
            await submit.click()
            await asyncio.sleep(4)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=20000)
            except Exception:
                pass
            return True
        return False
    except Exception as e:
        err_msg = str(e) or type(e).__name__
        print(f"[cyberleninka] CAPTCHA solver error: {err_msg}", flush=True)
        if not headless:
            print("[cyberleninka] Решите CAPTCHA вручную в окне браузера (ждём до 120 сек)...", flush=True)
            try:
                await page.wait_for_selector("div.ocr, div.main", timeout=120000)
                return True
            except Exception:
                return False
        return False


def _sync_playwright_fetch(url: str, timeout: int, user_agent: str | None = None, headless: bool = True) -> str:
    """Синхронная обёртка: свой event loop в потоке (обход NotImplementedError на Windows). При CAPTCHA — повтор через Firefox."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return ""
    ua = (user_agent or DEFAULT_USER_AGENT).strip()

    async def _fetch_with_browser(p, browser_kind: str):
        if browser_kind == "chromium":
            browser = await p.chromium.launch(
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--window-size=1920,1080",
                ],
            )
        else:
            browser = await p.firefox.launch(headless=headless)
        try:
            page = await browser.new_page(
                viewport={"width": 1920, "height": 1080},
                user_agent=ua,
                extra_http_headers={"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
            )
            if browser_kind == "chromium":
                await page.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)
            await asyncio.sleep(2)
            html = await page.content()
            if _is_captcha_page(html):
                print("[cyberleninka] CAPTCHA detected, trying solver...", flush=True)
                solved = await _try_solve_recaptcha_and_submit(page, headless)
                print(f"[cyberleninka] CAPTCHA solver result: {'ok' if solved else 'fail'}", flush=True)
                if solved:
                    await asyncio.sleep(4)
                    try:
                        await page.wait_for_selector("div.ocr, div.main", timeout=25000)
                    except Exception:
                        pass
                    await asyncio.sleep(2)
                    html = await page.content()
            else:
                try:
                    await page.wait_for_selector("div.ocr, div.main", timeout=20000)
                except Exception:
                    try:
                        await page.wait_for_selector("h1", timeout=8000)
                    except Exception:
                        pass
                await asyncio.sleep(2)
                html = await page.content()
            return html
        finally:
            await browser.close()

    async def _run():
        async with async_playwright() as p:
            html = await _fetch_with_browser(p, "chromium")
            if _is_captcha_page(html):
                try:
                    html = await _fetch_with_browser(p, "firefox")
                except Exception:
                    pass
            return html
    try:
        return asyncio.run(_run())
    except Exception:
        return ""


def _sync_playwright_search_ui(
    base_url: str, query: str, max_results: int, timeout: int, user_agent: str | None = None, headless: bool = True, date_from: str | None = None, date_to: str | None = None
) -> list:
    """Синхронная обёртка: главная -> ввод в поиск -> установка фильтра по датам -> ожидание блока фильтра -> сбор ссылок."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return []
    from urllib.parse import urljoin
    links = []
    ua = (user_agent or DEFAULT_USER_AGENT).strip()

    async def _run():
        nonlocal links
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            try:
                page = await browser.new_page(
                    user_agent=ua,
                    extra_http_headers={"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
                )
                # Начинаем со страницы поиска, где уже есть форма с фильтрами
                search_url = f"{base_url}/search?q=&page=1"
                await page.goto(
                    search_url,
                    wait_until="domcontentloaded",
                    timeout=timeout * 1000,
                )
                await asyncio.sleep(1.5)
                
                # ШАГ 1: Двойное нажатие кнопки поиска ДО ввода keywords, чтобы открыть форму фильтра
                # Селектор: #search-box-full > div > fieldset > input.search-btn
                search_btn_sel = (
                    "#search-box-full > div > fieldset > input.search-btn, "
                    "#search-box-full input.search-btn, "
                    "#search-box-full button.search-btn, "
                    ".search-btn, "
                    "input.search-btn"
                )
                initial_search_btn = await page.query_selector(search_btn_sel)
                if initial_search_btn:
                    # Первое нажатие
                    await initial_search_btn.click()
                    print("[cyberleninka] STEP 1a: first click on search button", flush=True)
                    await asyncio.sleep(1.0)
                    # Второе нажатие (двойное нажатие для надежного открытия формы фильтра)
                    initial_search_btn = await page.query_selector(search_btn_sel)
                    if initial_search_btn:
                        await initial_search_btn.click()
                        print("[cyberleninka] STEP 1b: second click on search button (double-click)", flush=True)
                    await asyncio.sleep(1.5)  # Ждём открытия формы фильтра
                
                search_sel = (
                    "input[name='q'], input[type='search'], .search input[type='text'], "
                    "input[placeholder*='поиск'], input[placeholder*='Поиск'], #search input, div.search input, "
                    "form[action*='/search'] input[type='text'], form[action*='/search'] input[name='q'], "
                    "#search-box-full input[type='text'], #search-box-full input[name='q']"
                )
                inp = await page.query_selector(search_sel)
                if not inp:
                    inp = await page.query_selector("input")
                if inp:
                    await inp.fill(query)
                    print(f"[cyberleninka] STEP 2: entered keywords: {query[:50]}", flush=True)
                    await asyncio.sleep(0.5)
                else:
                    print("[cyberleninka] WARNING: search input field not found", flush=True)
                
                # ШАГ 3: Если есть даты - заполняем поля фильтра по датам
                date_filter_applied = False
                if date_from or date_to:
                    try:
                        # Ищем блок фильтра
                        filter_block = await page.query_selector(
                            "div.infoblock.filter-block, "
                            ".filter-block, "
                            "div.search .infoblock, "
                            "#body .content .search .infoblock.filter-block"
                        )
                        if filter_block:
                            # Извлекаем год из даты (YYYY-MM-DD -> YYYY)
                            year_from = date_from[:4] if date_from and len(date_from) >= 4 else None
                            year_to = date_to[:4] if date_to and len(date_to) >= 4 else None
                            
                            # Ищем поля "от" и "до"
                            year_from_input = None
                            year_to_input = None
                            
                            selectors_from = [
                                "div.infoblock.filter-block input[placeholder*='от']",
                                "div.infoblock.filter-block input[name*='from']",
                                ".filter-block input[placeholder*='от']",
                                "div.search .infoblock input[placeholder*='от']",
                            ]
                            selectors_to = [
                                "div.infoblock.filter-block input[placeholder*='до']",
                                "div.infoblock.filter-block input[name*='to']",
                                ".filter-block input[placeholder*='до']",
                                "div.search .infoblock input[placeholder*='до']",
                            ]
                            
                            for sel in selectors_from:
                                year_from_input = await page.query_selector(sel)
                                if year_from_input:
                                    break
                            
                            for sel in selectors_to:
                                year_to_input = await page.query_selector(sel)
                                if year_to_input:
                                    break
                            
                            # Если не нашли по селекторам, ищем все input в блоке фильтра
                            if not year_from_input or not year_to_input:
                                all_inputs = await page.query_selector_all(
                                    "div.infoblock.filter-block input[type='text'], "
                                    "div.infoblock.filter-block input[type='number'], "
                                    ".filter-block input[type='text'], "
                                    ".filter-block input[type='number']"
                                )
                                for inp_date in all_inputs:
                                    placeholder = await inp_date.get_attribute("placeholder") or ""
                                    name = await inp_date.get_attribute("name") or ""
                                    if not year_from_input and ("от" in placeholder.lower() or "from" in name.lower()):
                                        year_from_input = inp_date
                                    elif not year_to_input and ("до" in placeholder.lower() or "to" in name.lower()):
                                        year_to_input = inp_date
                                if len(all_inputs) >= 2:
                                    if not year_from_input:
                                        year_from_input = all_inputs[0]
                                    if not year_to_input:
                                        year_to_input = all_inputs[1]
                            
                            # Заполняем поля дат
                            if year_from_input and year_from:
                                await year_from_input.fill(year_from)
                                print(f"[cyberleninka] STEP 3: filled date_from field: {year_from}", flush=True)
                                await asyncio.sleep(0.3)
                            if year_to_input and year_to:
                                await year_to_input.fill(year_to)
                                print(f"[cyberleninka] STEP 3: filled date_to field: {year_to}", flush=True)
                                await asyncio.sleep(0.3)
                            
                            if (year_from_input and year_from) or (year_to_input and year_to):
                                date_filter_applied = True
                                
                                # ШАГ 3b: Нажимаем кнопку "Задать" для применения фильтра по датам
                                zadat_btn_selectors = [
                                    "#body > div.content > div > div.search > div.infoblock.filter-block > div > div:nth-child(1) > ul > li.active > button",
                                    "div.infoblock.filter-block div > div:nth-child(1) > ul > li.active > button",
                                    "div.infoblock.filter-block ul > li.active > button",
                                    "div.infoblock.filter-block button:has-text('Задать')",
                                    "div.infoblock.filter-block input[type='submit'][value*='Задать']",
                                    ".filter-block button:has-text('Задать')",
                                    "div.search .infoblock button:has-text('Задать')",
                                ]
                                zadat_btn = None
                                for sel in zadat_btn_selectors:
                                    try:
                                        zadat_btn = await page.query_selector(sel)
                                        if zadat_btn:
                                            print(f"[cyberleninka] found 'Задать' button with selector: {sel[:80]}", flush=True)
                                            break
                                    except Exception:
                                        continue
                                
                                if zadat_btn:
                                    await zadat_btn.click()
                                    print(f"[cyberleninka] STEP 3b: clicked 'Задать' button to apply date filter", flush=True)
                                    await asyncio.sleep(2.0)  # Ждём применения фильтра и загрузки результатов
                                else:
                                    print(f"[cyberleninka] WARNING: 'Задать' button not found for date filter", flush=True)
                    except Exception as e:
                        print(f"[cyberleninka] failed to set date filter: {e}", flush=True)
                        import traceback
                        traceback.print_exc()
                
                # Если дат нет, нажимаем кнопку поиска после ввода keywords
                if not date_filter_applied:
                    search_btn_after_keywords = await page.query_selector(search_btn_sel)
                    if search_btn_after_keywords:
                        await search_btn_after_keywords.click()
                        print(f"[cyberleninka] STEP 3: clicked search button after entering keywords (no date filter)", flush=True)
                        await asyncio.sleep(2.5)  # Ждём загрузки результатов
                
                # После нажатия "Задать" (или кнопки поиска, если дат нет) парсим результаты
                await asyncio.sleep(1.0)  # Дополнительное ожидание для загрузки результатов
                block_sel = (
                    "div.infoblock.filter-block, div.search div.infoblock, "
                    "#body .content .search .infoblock.filter-block, div.search a[href*='/article/n/']"
                )
                try:
                    await page.wait_for_selector(block_sel, timeout=8000)
                except Exception:
                    pass
                await asyncio.sleep(0.8)
                
                # Пагинация: собираем ссылки со всех страниц до достижения max_results
                page_num = 1
                seen = set()
                while len(links) < max_results:
                    content = await page.content()
                    soup = BeautifulSoup(content, "html.parser")
                    
                    # Парсим ссылки с текущей страницы
                    page_links_count = 0
                    for a in soup.select("a[href*='/article/n/']"):
                        href = a.get("href")
                        if not href or "article/c/" in href:
                            continue
                        full = urljoin(base_url, href).rstrip("/")
                        if full in seen:
                            continue
                        seen.add(full)
                        title = a.get_text(strip=True) or "Статья"
                        links.append((title, full))
                        page_links_count += 1
                        if len(links) >= max_results:
                            break
                    
                    for a in soup.select("div.infoblock.filter-block a[href*='/article/'], div.search a[href*='/article/']"):
                        if len(links) >= max_results:
                            break
                        href = a.get("href")
                        if not href or "article/c/" in href:
                            continue
                        full = urljoin(base_url, href).rstrip("/")
                        if full in seen:
                            continue
                        seen.add(full)
                        links.append((a.get_text(strip=True) or "Статья", full))
                        page_links_count += 1
                    
                    print(f"[cyberleninka] page {page_num}: found {page_links_count} links, total: {len(links)}/{max_results}", flush=True)
                    
                    # Если собрали достаточно ссылок или на странице меньше 10 ссылок (конец результатов)
                    if len(links) >= max_results or page_links_count < 10:
                        break
                    
                    # Переходим на следующую страницу через кнопку пагинации (чтобы сохранить фильтр по датам)
                    page_num += 1
                    next_page_found = False
                    
                    # Ждём загрузки JavaScript и появления пагинации
                    await asyncio.sleep(2.0)  # Даём время на загрузку JavaScript
                    
                    # Прокручиваем страницу вниз, чтобы пагинация загрузилась (если она внизу)
                    try:
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1.5)
                    except Exception:
                        pass
                    
                    # Пытаемся найти пагинацию через JavaScript (она может быть создана динамически)
                    try:
                        # Ищем все ссылки, которые содержат page= в href через JavaScript
                        pagination_hrefs = await page.evaluate("""
                            () => {
                                const links = Array.from(document.querySelectorAll('a[href*="page="], a[href*="/search?"]'));
                                return links.map(link => ({
                                    href: link.href,
                                    text: link.innerText.trim(),
                                    visible: link.offsetParent !== null
                                })).filter(link => link.visible && link.href.includes('page='));
                            }
                        """)
                        print(f"[cyberleninka] found {len(pagination_hrefs)} pagination links via JavaScript", flush=True)
                        
                        # Ищем ссылку на нужную страницу
                        for link_info in pagination_hrefs:
                            href = link_info.get('href', '')
                            text = link_info.get('text', '')
                            if href and (f"page={page_num}" in href.lower() or 
                                        (text == str(page_num) and "page=" in href.lower())):
                                # Кликаем через JavaScript
                                clicked = await page.evaluate(f"""
                                    () => {{
                                        const link = Array.from(document.querySelectorAll('a[href*="page="]')).find(a => 
                                            a.href.includes('page={page_num}') || 
                                            (a.innerText.trim() === '{page_num}' && a.href.includes('page='))
                                        );
                                        if (link) {{
                                            link.click();
                                            return true;
                                        }}
                                        return false;
                                    }}
                                """)
                                if clicked:
                                    print(f"[cyberleninka] clicked pagination link to page {page_num} via JavaScript (href: {href[:100]})", flush=True)
                                    next_page_found = True
                                    await asyncio.sleep(2.5)
                                    try:
                                        await page.wait_for_selector(block_sel, timeout=5000)
                                    except Exception:
                                        pass
                                    break
                    except Exception as e:
                        print(f"[cyberleninka] error finding pagination via JavaScript: {e}", flush=True)
                    
                    # Если не нашли через JavaScript, пробуем обычный поиск
                    if not next_page_found:
                        try:
                            # Расширенный поиск всех возможных элементов пагинации
                            pagination_links = await page.query_selector_all(
                                ".pagination a, "
                                ".pager a, "
                                ".pages a, "
                                "div.pagination a, "
                                "ul.pagination a, "
                                "ul.pager a, "
                                "nav.pagination a, "
                                "nav.pager a, "
                                ".pagination li a, "
                                ".pager li a, "
                                "a[href*='page='], "
                                "a[href*='/search?'], "
                                "a[href*='&page='], "
                                "a[href*='?page='], "
                                "div.search a[href*='page='], "
                                ".content a[href*='page=']"
                            )
                            print(f"[cyberleninka] found {len(pagination_links)} potential pagination links via selectors", flush=True)
                            
                            for link in pagination_links:
                                try:
                                    href = await link.get_attribute("href")
                                    text = await link.inner_text()
                                    is_visible = await link.is_visible()
                                    
                                    if href and is_visible:
                                        # Проверяем разные варианты href для следующей страницы
                                        href_lower = href.lower()
                                        if (f"page={page_num}" in href_lower or 
                                            f"&page={page_num}" in href_lower or
                                            f"?page={page_num}" in href_lower or
                                            (text.strip() == str(page_num) and ("page=" in href_lower or "/search" in href_lower))):
                                            await link.scroll_into_view_if_needed()
                                            await asyncio.sleep(0.3)
                                            await link.click()
                                            print(f"[cyberleninka] clicked pagination link to page {page_num} (href: {href[:100]}, text: {text.strip()})", flush=True)
                                            next_page_found = True
                                            await asyncio.sleep(2.5)  # Ждём загрузки следующей страницы
                                            try:
                                                await page.wait_for_selector(block_sel, timeout=5000)
                                            except Exception:
                                                pass
                                            break
                                except Exception as e:
                                    continue
                        except Exception as e:
                            print(f"[cyberleninka] error finding pagination links via selectors: {e}", flush=True)
                            import traceback
                            traceback.print_exc()
                    
                    # Если не нашли по href, ищем по тексту
                    if not next_page_found:
                        next_page_selectors = [
                            f"a:has-text('{page_num}')",
                            "a:has-text('Следующая')",
                            "a:has-text('Далее')",
                            "a:has-text('→')",
                            "a:has-text('>')",
                            ".pagination a:has-text('→')",
                            ".pagination a:has-text('>')",
                            ".pager a:has-text('→')",
                            ".pager a:has-text('>')",
                        ]
                        for sel in next_page_selectors:
                            try:
                                next_link = await page.query_selector(sel)
                                if next_link:
                                    # Проверяем, что это действительно ссылка на следующую страницу
                                    href = await next_link.get_attribute("href")
                                    if href and ("page=" in href or "/search" in href):
                                        await next_link.click()
                                        print(f"[cyberleninka] clicked pagination link to page {page_num} (selector: {sel[:60]})", flush=True)
                                        next_page_found = True
                                        await asyncio.sleep(2.5)  # Ждём загрузки следующей страницы
                                        try:
                                            await page.wait_for_selector(block_sel, timeout=5000)
                                        except Exception:
                                            pass
                                        break
                            except Exception:
                                continue
                    
                    if not next_page_found:
                        # Сохраняем HTML для отладки структуры пагинации
                        try:
                            debug_dir = Path("debug")
                            debug_dir.mkdir(exist_ok=True)
                            content_debug = await page.content()
                            (debug_dir / f"cyberleninka_pagination_page_{page_num-1}.html").write_text(
                                content_debug, encoding="utf-8"
                            )
                            print(f"[cyberleninka] saved HTML to debug/cyberleninka_pagination_page_{page_num-1}.html for debugging", flush=True)
                        except Exception:
                            pass
                        
                        print(f"[cyberleninka] WARNING: pagination button for page {page_num} not found, using direct navigation with filter restoration", flush=True)
                        # Fallback: прямой переход с восстановлением фильтров
                        from urllib.parse import quote
                        try:
                            next_page_url = f"{base_url}/search?q={quote(query)}&page={page_num}"
                            await page.goto(
                                next_page_url,
                                wait_until="domcontentloaded",
                                timeout=timeout * 1000,
                            )
                            await asyncio.sleep(2.0)  # Ждём загрузки страницы
                            
                            # Ждём появления элементов
                            try:
                                await page.wait_for_selector(block_sel, timeout=5000)
                            except Exception:
                                pass
                            
                            # Пытаемся восстановить фильтры по датам
                            if date_from or date_to:
                                await asyncio.sleep(2.0)  # Даём время на загрузку фильтров
                                try:
                                    year_from = date_from[:4] if date_from and len(date_from) >= 4 else None
                                    year_to = date_to[:4] if date_to and len(date_to) >= 4 else None
                                    
                                    # Ждём появления блока фильтра с таймаутом
                                    filter_block = None
                                    try:
                                        await page.wait_for_selector(
                                            "div.infoblock.filter-block, .filter-block, div.search .infoblock",
                                            timeout=5000
                                        )
                                    except Exception:
                                        pass
                                    
                                    # Ищем блок фильтра (максимум 5 попыток)
                                    for attempt in range(5):
                                        filter_block = await page.query_selector(
                                            "div.infoblock.filter-block, .filter-block, div.search .infoblock, "
                                            "div.search div.infoblock, #body .content .search .infoblock.filter-block"
                                        )
                                        if filter_block:
                                            break
                                        if attempt < 4:
                                            await asyncio.sleep(0.8)
                                    
                                    if filter_block:
                                        # Ждём появления полей ввода
                                        await asyncio.sleep(1.0)
                                        
                                        # Ищем поля ввода дат - пробуем разные селекторы
                                        all_inputs = []
                                        selectors_list = [
                                            "div.infoblock.filter-block input[type='text'], div.infoblock.filter-block input[type='number']",
                                            ".filter-block input[type='text'], .filter-block input[type='number']",
                                            "div.search .infoblock input[type='text'], div.search .infoblock input[type='number']",
                                            "div.infoblock.filter-block input[placeholder*='от'], div.infoblock.filter-block input[placeholder*='до']",
                                            ".filter-block input[placeholder*='от'], .filter-block input[placeholder*='до']",
                                            "div.infoblock.filter-block ul input",
                                            ".filter-block ul input",
                                        ]
                                        
                                        for selector in selectors_list:
                                            all_inputs = await page.query_selector_all(selector)
                                            if len(all_inputs) >= 2:
                                                break
                                        
                                        # Если не нашли по селекторам, ищем все input в блоке фильтра
                                        if len(all_inputs) < 2:
                                            all_inputs = await page.query_selector_all(
                                                "div.infoblock.filter-block input, .filter-block input, div.search .infoblock input"
                                            )
                                        
                                        print(f"[cyberleninka] found {len(all_inputs)} input fields in filter block", flush=True)
                                        
                                        if len(all_inputs) >= 2:
                                            # Заполняем первое поле (от)
                                            if year_from:
                                                try:
                                                    await all_inputs[0].fill(year_from)
                                                    await asyncio.sleep(0.3)
                                                    print(f"[cyberleninka] filled date_from field: {year_from}", flush=True)
                                                except Exception as e:
                                                    print(f"[cyberleninka] failed to fill date_from: {e}", flush=True)
                                            
                                            # Заполняем второе поле (до)
                                            if year_to:
                                                try:
                                                    await all_inputs[1].fill(year_to)
                                                    await asyncio.sleep(0.3)
                                                    print(f"[cyberleninka] filled date_to field: {year_to}", flush=True)
                                                except Exception as e:
                                                    print(f"[cyberleninka] failed to fill date_to: {e}", flush=True)
                                            
                                            # Нажимаем "Задать"
                                            zadat_btn = None
                                            zadat_selectors = [
                                                "div.infoblock.filter-block button:has-text('Задать')",
                                                ".filter-block button:has-text('Задать')",
                                                "div.infoblock.filter-block ul > li button:has-text('Задать')",
                                                ".filter-block ul > li button:has-text('Задать')",
                                                "div.infoblock.filter-block button span:has-text('Задать')",
                                                "button:has-text('Задать')",
                                            ]
                                            
                                            for sel in zadat_selectors:
                                                try:
                                                    zadat_btn = await page.query_selector(sel)
                                                    if zadat_btn:
                                                        break
                                                except Exception:
                                                    continue
                                            
                                            if zadat_btn:
                                                await zadat_btn.click()
                                                print(f"[cyberleninka] restored date filters after navigation to page {page_num}", flush=True)
                                                await asyncio.sleep(4.0)  # Ждём применения фильтра и загрузки результатов
                                                
                                                # После применения фильтра страница возвращается на страницу 1
                                                # Проверяем текущую страницу и используем кнопки пагинации для перехода на страницу 2
                                                if page_num > 1:
                                                    print(f"[cyberleninka] filter applied, checking current page and navigating to page {page_num}", flush=True)
                                                    
                                                    # Проверяем текущую страницу через JavaScript
                                                    try:
                                                        current_page_text = await page.evaluate("""
                                                            () => {
                                                                const span = document.querySelector('h1.bigheader span');
                                                                if (span) {
                                                                    const match = span.textContent.match(/страница\\s+(\\d+)/);
                                                                    return match ? parseInt(match[1]) : null;
                                                                }
                                                                return null;
                                                            }
                                                        """)
                                                        print(f"[cyberleninka] current page detected: {current_page_text}", flush=True)
                                                    except Exception:
                                                        current_page_text = None
                                                    
                                                    # Если мы на странице 1, пытаемся использовать кнопки пагинации
                                                    if current_page_text == 1:
                                                        print(f"[cyberleninka] on page 1 after filter, trying to use pagination buttons to go to page {page_num}", flush=True)
                                                        
                                                        # Прокручиваем вниз, чтобы кнопки пагинации были видны
                                                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                                        await asyncio.sleep(1.0)
                                                        
                                                        # Ищем кнопки пагинации через JavaScript
                                                        pagination_clicked = False
                                                        try:
                                                            pagination_clicked = await page.evaluate(f"""
                                                                () => {{
                                                                    // Ищем ссылку на нужную страницу
                                                                    const links = Array.from(document.querySelectorAll('a[href*="page={page_num}"]'));
                                                                    for (const link of links) {{
                                                                        const href = link.getAttribute('href');
                                                                        if (href && href.includes('page={page_num}')) {{
                                                                            link.click();
                                                                            return true;
                                                                        }}
                                                                    }}
                                                                    // Ищем кнопку "Следующая" или "→"
                                                                    const nextButtons = Array.from(document.querySelectorAll('a')).filter(a => {{
                                                                        const text = a.textContent.trim();
                                                                        return text === 'Следующая' || text === '→' || text === '>' || text === '{page_num}';
                                                                    }});
                                                                    if (nextButtons.length > 0) {{
                                                                        nextButtons[0].click();
                                                                        return true;
                                                                    }}
                                                                    return false;
                                                                }}
                                                            """)
                                                            if pagination_clicked:
                                                                print(f"[cyberleninka] clicked pagination button via JavaScript to go to page {page_num}", flush=True)
                                                                await asyncio.sleep(3.0)
                                                                await page.wait_for_selector("ul.list, ul#search-results, div.search ul", timeout=8000)
                                                        except Exception as e:
                                                            print(f"[cyberleninka] failed to click pagination via JavaScript: {e}", flush=True)
                                                        
                                                        # Если кнопки пагинации не сработали, используем прямой переход
                                                        if not pagination_clicked:
                                                            print(f"[cyberleninka] pagination buttons not found, using direct URL navigation", flush=True)
                                                            from urllib.parse import quote
                                                            target_page_url = f"{base_url}/search?q={quote(query)}&page={page_num}"
                                                            try:
                                                                await page.goto(
                                                                    target_page_url,
                                                                    wait_until="domcontentloaded",
                                                                    timeout=timeout * 1000,
                                                                )
                                                                await asyncio.sleep(2.0)
                                                                
                                                                # Снова восстанавливаем фильтры (они могли сброситься при переходе)
                                                                if date_from or date_to:
                                                                    await asyncio.sleep(1.0)
                                                                    try:
                                                                        year_from = date_from[:4] if date_from and len(date_from) >= 4 else None
                                                                        year_to = date_to[:4] if date_to and len(date_to) >= 4 else None
                                                                        
                                                                        filter_block = await page.query_selector(
                                                                            "div.infoblock.filter-block, .filter-block, div.search .infoblock"
                                                                        )
                                                                        if filter_block:
                                                                            all_inputs = await page.query_selector_all(
                                                                                "div.infoblock.filter-block input, .filter-block input, div.search .infoblock input"
                                                                            )
                                                                            if len(all_inputs) >= 2:
                                                                                if year_from:
                                                                                    await all_inputs[0].fill(year_from)
                                                                                if year_to:
                                                                                    await all_inputs[1].fill(year_to)
                                                                                
                                                                                zadat_btn2 = await page.query_selector(
                                                                                    "div.infoblock.filter-block button:has-text('Задать'), .filter-block button:has-text('Задать')"
                                                                                )
                                                                                if zadat_btn2:
                                                                                    await zadat_btn2.click()
                                                                                    print(f"[cyberleninka] re-applied date filters on page {page_num}", flush=True)
                                                                                    await asyncio.sleep(3.0)
                                                                    except Exception as e:
                                                                        print(f"[cyberleninka] failed to re-apply filters: {e}", flush=True)
                                                                
                                                                # Ждём появления результатов
                                                                try:
                                                                    await page.wait_for_selector("ul.list, ul#search-results, div.search ul", timeout=8000)
                                                                except Exception:
                                                                    pass
                                                                
                                                                await asyncio.sleep(1.0)
                                                            except Exception as e:
                                                                print(f"[cyberleninka] failed to navigate back to page {page_num}: {e}", flush=True)
                                                
                                                # Прокручиваем страницу несколько раз для загрузки всех элементов
                                                for scroll_attempt in range(3):
                                                    try:
                                                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                                        await asyncio.sleep(0.8)
                                                        await page.evaluate("window.scrollTo(0, 0)")
                                                        await asyncio.sleep(0.5)
                                                    except Exception:
                                                        pass
                                                
                                                # После восстановления фильтров заново получаем контент и парсим ссылки
                                                print(f"[cyberleninka] re-parsing page {page_num} after filter restoration", flush=True)
                                                content_after_filter = await page.content()
                                                
                                                soup_after_filter = BeautifulSoup(content_after_filter, "html.parser")
                                                
                                                # Парсим ссылки с обновлённой страницы - пробуем разные селекторы
                                                page_links_count_after = 0
                                                
                                                # Селектор 1: все ссылки на статьи (включая внутри li h2)
                                                for a in soup_after_filter.select("a[href*='/article/n/']"):
                                                    href = a.get("href")
                                                    if not href or "article/c/" in href:
                                                        continue
                                                    full = urljoin(base_url, href).rstrip("/")
                                                    if full not in seen:
                                                        seen.add(full)
                                                        title = a.get_text(strip=True) or "Статья"
                                                        links.append((title, full))
                                                        page_links_count_after += 1
                                                        if len(links) >= max_results:
                                                            break
                                                
                                                # Селектор 2: ссылки в списке результатов (ul.list li h2 a)
                                                if page_links_count_after == 0:
                                                    for a in soup_after_filter.select("ul.list li h2 a[href*='/article/'], ul#search-results li h2 a[href*='/article/'], div.search ul li h2 a[href*='/article/']"):
                                                        if len(links) >= max_results:
                                                            break
                                                        href = a.get("href")
                                                        if not href or "article/c/" in href:
                                                            continue
                                                        full = urljoin(base_url, href).rstrip("/")
                                                        if full not in seen:
                                                            seen.add(full)
                                                            links.append((a.get_text(strip=True) or "Статья", full))
                                                            page_links_count_after += 1
                                                
                                                # Селектор 3: ссылки в блоке фильтра и поиска
                                                for a in soup_after_filter.select("div.infoblock.filter-block a[href*='/article/'], div.search a[href*='/article/']"):
                                                    if len(links) >= max_results:
                                                        break
                                                    href = a.get("href")
                                                    if not href or "article/c/" in href:
                                                        continue
                                                    full = urljoin(base_url, href).rstrip("/")
                                                    if full not in seen:
                                                        seen.add(full)
                                                        links.append((a.get_text(strip=True) or "Статья", full))
                                                        page_links_count_after += 1
                                                
                                                print(f"[cyberleninka] page {page_num} after filter restoration: found {page_links_count_after} links, total: {len(links)}/{max_results}", flush=True)
                                                
                                                # Если собрали достаточно ссылок, выходим из цикла
                                                if len(links) >= max_results:
                                                    break
                                                
                                                # Если на странице меньше 10 ссылок, это может быть конец результатов
                                                if page_links_count_after < 10:
                                                    print(f"[cyberleninka] page {page_num}: only {page_links_count_after} links found, stopping pagination", flush=True)
                                                    break
                                                
                                                # Переходим к следующей странице
                                                page_num += 1
                                                continue
                                            else:
                                                print(f"[cyberleninka] WARNING: 'Задать' button not found", flush=True)
                                        else:
                                            print(f"[cyberleninka] WARNING: date input fields not found (found {len(all_inputs)} inputs)", flush=True)
                                    else:
                                        print(f"[cyberleninka] WARNING: filter block not found", flush=True)
                                except Exception as e:
                                    print(f"[cyberleninka] failed to restore date filters: {e}", flush=True)
                                    import traceback
                                    traceback.print_exc()
                            
                            await asyncio.sleep(0.5)
                            print(f"[cyberleninka] navigated to page {page_num}", flush=True)
                            
                            # После восстановления фильтров нужно заново получить контент страницы
                            # Продолжаем цикл пагинации с обновлённым контентом
                            continue
                        except Exception as e:
                            print(f"[cyberleninka] failed to navigate to page {page_num}: {e}", flush=True)
                            break
            finally:
                await browser.close()

    try:
        asyncio.run(_run())
    except Exception:
        pass
    return links[:max_results]


def _cyberleninka_config(key: str, default):
    """Читает config/settings.yaml -> sources.cyberleninka.<key>."""
    try:
        from pathlib import Path
        import yaml
        root = Path(__file__).resolve().parent.parent.parent
        cfg_path = root / "config" / "settings.yaml"
        if cfg_path.exists():
            with open(cfg_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            val = (data.get("sources") or {}).get("cyberleninka") or {}
            return val.get(key, default)
    except Exception:
        pass
    return default


def _text_processing_config(key: str, default):
    """Читает config/settings.yaml -> text_processing.<key>."""
    try:
        from pathlib import Path
        import yaml
        root = Path(__file__).resolve().parent.parent.parent
        cfg_path = root / "config" / "settings.yaml"
        if cfg_path.exists():
            with open(cfg_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            val = (data.get("text_processing") or {}).get(key, default)
            return val
        return default
    except Exception:
        return default


class CyberLeninkaSource(BaseSource):
    """Источник CyberLeninka: поиск и извлечение статей."""

    name = "cyberleninka"

    def __init__(
        self,
        base_url: str = "https://cyberleninka.ru",
        request_delay: float = 3.0,
        timeout: int = 30,
        max_retries: int = 3,
        use_playwright: Optional[bool] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.request_delay = request_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.use_playwright = use_playwright if use_playwright is not None else _cyberleninka_config("use_playwright", True)
        self.playwright_headless = _cyberleninka_config("playwright_headless", True)
        self.user_agent = (user_agent or _cyberleninka_config("user_agent", None) or DEFAULT_USER_AGENT).strip()

    def _search_url(self, query: str, page: int = 1) -> str:
        """URL страницы поиска. В q передаются только ключевые слова (без операторов)."""
        if not query or not query.strip():
            return f"{self.base_url}/search?page={page}"
        return f"{self.base_url}/search?{urlencode({'q': query.strip(), 'page': page}, encoding='utf-8')}"

    def _date_in_range(
        self,
        date_str: str | None,
        date_from: str | None,
        date_to: str | None,
    ) -> bool:
        """Проверяет, что дата документа попадает в диапазон [date_from, date_to] (включительно)."""
        if date_from is None and date_to is None:
            return True
        normalized = normalize_date(date_str) if date_str else None
        if not normalized:
            return False
        if date_from and normalized < date_from:
            return False
        if date_to and normalized > date_to:
            return False
        return True

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        """GET с ретраями и задержкой."""
        for attempt in range(self.max_retries):
            try:
                await asyncio.sleep(self.request_delay)
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                    headers={
                        "User-Agent": self.user_agent,
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                        "Referer": f"{self.base_url}/",
                    },
                ) as resp:
                    resp.raise_for_status()
                    html = await resp.text(encoding="utf-8", errors="replace")
                    if _is_captcha_page(html):
                        raise SourceTemporarilyUnavailableError(
                            "CAPTCHA required",
                            source_name="cyberleninka",
                        )
                    return html
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        return ""

    async def _fetch_with_playwright(self, url: str) -> str:
        """Загрузка страницы через браузер. На Windows запускаем в отдельном потоке (свой event loop)."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return ""
        if sys.platform == "win32":
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _sync_playwright_fetch, url, self.timeout, self.user_agent, self.playwright_headless)
        html = ""
        try:
            async with async_playwright() as p:
                for browser_kind in ("chromium", "firefox"):
                    try:
                        if browser_kind == "chromium":
                            browser = await p.chromium.launch(
                                headless=self.playwright_headless,
                                args=[
                                    "--disable-blink-features=AutomationControlled",
                                    "--disable-dev-shm-usage",
                                    "--no-sandbox",
                                    "--window-size=1920,1080",
                                ],
                            )
                        else:
                            browser = await p.firefox.launch(headless=self.playwright_headless)
                        try:
                            page = await browser.new_page(
                                viewport={"width": 1920, "height": 1080},
                                user_agent=self.user_agent,
                                extra_http_headers={"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
                            )
                            if browser_kind == "chromium":
                                await page.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
                            await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout * 1000)
                            await asyncio.sleep(2)
                            html = await page.content()
                            if _is_captcha_page(html):
                                print("[cyberleninka] CAPTCHA detected, trying solver...", flush=True)
                                solved = await _try_solve_recaptcha_and_submit(page, self.playwright_headless)
                                print(f"[cyberleninka] CAPTCHA solver result: {'ok' if solved else 'fail'}", flush=True)
                                if solved:
                                    await asyncio.sleep(4)
                                    try:
                                        await page.wait_for_selector("div.ocr, div.main", timeout=25000)
                                    except Exception:
                                        pass
                                    await asyncio.sleep(2)
                                    html = await page.content()
                            if not _is_captcha_page(html):
                                return html
                            try:
                                await page.wait_for_selector("div.ocr, div.main", timeout=20000)
                            except Exception:
                                try:
                                    await page.wait_for_selector("h1", timeout=8000)
                                except Exception:
                                    pass
                            await asyncio.sleep(2)
                            html = await page.content()
                            if not _is_captcha_page(html):
                                return html
                            raise SourceTemporarilyUnavailableError(
                                "CAPTCHA required (solver failed or not available)",
                                source_name="cyberleninka",
                            )
                        finally:
                            await browser.close()
                    except Exception:
                        if browser_kind == "firefox":
                            pass
        except SourceTemporarilyUnavailableError:
            raise
        except Exception:
            pass
        return html

    async def _fetch_article_html(self, session: aiohttp.ClientSession, url: str) -> str:
        """Загрузка HTML страницы статьи: через Playwright при use_playwright, иначе aiohttp. При пустом ответе Playwright — fallback на aiohttp."""
        html = ""
        if self.use_playwright:
            html = await self._fetch_with_playwright(url)
        if not html or len(html) < 500:
            html = await self._fetch(session, url)
        return html or ""

    async def _search_via_playwright_ui(
        self, query: str, max_results: int, date_from: str | None = None, date_to: str | None = None
    ) -> list[tuple[str, str]]:
        """
        Поиск через UI: главная -> ввод keywords в строку поиска -> установка фильтра по датам -> ожидание блока фильтра/результатов -> сбор ссылок.
        Селекторы: строка поиска, затем #body .content .search .infoblock.filter-block и ссылки на статьи.
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return []
        if sys.platform == "win32":
            # Для Windows используем синхронную обёртку с поддержкой фильтра по датам
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, _sync_playwright_search_ui, self.base_url, query, max_results, self.timeout, self.user_agent, self.playwright_headless, date_from, date_to
            )
        return await self._do_playwright_search_ui(query, max_results, date_from, date_to)

    async def _do_playwright_search_ui(
        self, query: str, max_results: int, date_from: str | None = None, date_to: str | None = None
    ) -> list[tuple[str, str]]:
        """Ввод в поисковую строку, установка фильтра по датам, ожидание результатов, парсинг ссылок из блока фильтра/результатов."""
        from playwright.async_api import async_playwright
        try:
            # Устанавливаем общий таймаут для всей операции Playwright
            async def _run_playwright():
                links = []  # Объявляем links внутри функции
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=self.playwright_headless)
                    try:
                        page = await browser.new_page(
                            user_agent=self.user_agent,
                            extra_http_headers={"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
                        )
                        # Начинаем со страницы поиска, где уже есть форма с фильтрами
                        search_url = f"{self.base_url}/search?q=&page=1"
                        await page.goto(
                            search_url,
                            wait_until="domcontentloaded",
                            timeout=self.timeout * 1000,
                        )
                        await asyncio.sleep(1.5)
                        
                        # ШАГ 1: Двойное нажатие кнопки поиска ДО ввода keywords, чтобы открыть форму фильтра
                        # Селектор: #search-box-full > div > fieldset > input.search-btn
                        search_btn_selector = (
                            "#search-box-full > div > fieldset > input.search-btn, "
                            "#search-box-full input.search-btn, "
                            "#search-box-full button.search-btn, "
                            ".search-btn, "
                            "input.search-btn"
                        )
                        initial_search_btn = await page.query_selector(search_btn_selector)
                        if initial_search_btn:
                            # Первое нажатие
                            await initial_search_btn.click()
                            print("[cyberleninka] STEP 1a: first click on search button", flush=True)
                            await asyncio.sleep(1.0)
                            # Второе нажатие (двойное нажатие для надежного открытия формы фильтра)
                            initial_search_btn = await page.query_selector(search_btn_selector)
                            if initial_search_btn:
                                await initial_search_btn.click()
                                print("[cyberleninka] STEP 1b: second click on search button (double-click)", flush=True)
                            await asyncio.sleep(1.5)  # Ждём открытия формы фильтра
                        
                        # ШАГ 2: Вводим keywords в поле поиска
                        search_sel = (
                            "input[name='q'], input[type='search'], "
                            ".search input[type='text'], input[placeholder*='поиск'], "
                            "input[placeholder*='Поиск'], #search input, div.search input, "
                            "form[action*='/search'] input[type='text'], form[action*='/search'] input[name='q'], "
                            "#search-box-full input[type='text'], #search-box-full input[name='q']"
                        )
                        inp = await page.query_selector(search_sel)
                        if not inp:
                            inp = await page.query_selector("input")
                        if inp:
                            await inp.fill(query)
                            print(f"[cyberleninka] STEP 2: entered keywords: {query[:50]}", flush=True)
                            await asyncio.sleep(0.5)
                        else:
                            print("[cyberleninka] WARNING: search input field not found", flush=True)
                        
                        # ШАГ 3: Если есть даты - заполняем поля фильтра по датам ПЕРЕД нажатием кнопки поиска
                        # (это делается здесь, чтобы все данные были введены перед отправкой формы)
                        date_filter_applied = False
                        if date_from or date_to:
                            try:
                                # Ищем блок фильтра
                                filter_block = await page.query_selector(
                                    "div.infoblock.filter-block, "
                                    ".filter-block, "
                                    "div.search .infoblock, "
                                    "#body .content .search .infoblock.filter-block"
                                )
                                if filter_block:
                                    # Извлекаем год из даты (YYYY-MM-DD -> YYYY)
                                    year_from = date_from[:4] if date_from and len(date_from) >= 4 else None
                                    year_to = date_to[:4] if date_to and len(date_to) >= 4 else None
                                    
                                    # Ищем поля "от" и "до"
                                    year_from_input = None
                                    year_to_input = None
                                    
                                    selectors_from = [
                                        "div.infoblock.filter-block input[placeholder*='от']",
                                        "div.infoblock.filter-block input[name*='from']",
                                        ".filter-block input[placeholder*='от']",
                                        "div.search .infoblock input[placeholder*='от']",
                                    ]
                                    selectors_to = [
                                        "div.infoblock.filter-block input[placeholder*='до']",
                                        "div.infoblock.filter-block input[name*='to']",
                                        ".filter-block input[placeholder*='до']",
                                        "div.search .infoblock input[placeholder*='до']",
                                    ]
                                    
                                    for sel in selectors_from:
                                        year_from_input = await page.query_selector(sel)
                                        if year_from_input:
                                            break
                                    
                                    for sel in selectors_to:
                                        year_to_input = await page.query_selector(sel)
                                        if year_to_input:
                                            break
                                    
                                    # Если не нашли по селекторам, ищем все input в блоке фильтра
                                    if not year_from_input or not year_to_input:
                                        all_inputs = await page.query_selector_all(
                                            "div.infoblock.filter-block input[type='text'], "
                                            "div.infoblock.filter-block input[type='number'], "
                                            ".filter-block input[type='text'], "
                                            ".filter-block input[type='number']"
                                        )
                                        for inp_date in all_inputs:
                                            placeholder = await inp_date.get_attribute("placeholder") or ""
                                            name = await inp_date.get_attribute("name") or ""
                                            if not year_from_input and ("от" in placeholder.lower() or "from" in name.lower()):
                                                year_from_input = inp_date
                                            elif not year_to_input and ("до" in placeholder.lower() or "to" in name.lower()):
                                                year_to_input = inp_date
                                        if len(all_inputs) >= 2:
                                            if not year_from_input:
                                                year_from_input = all_inputs[0]
                                            if not year_to_input:
                                                year_to_input = all_inputs[1]
                                    
                                    # Заполняем поля дат
                                    if year_from_input and year_from:
                                        await year_from_input.fill(year_from)
                                        print(f"[cyberleninka] STEP 3: filled date_from field: {year_from}", flush=True)
                                        await asyncio.sleep(0.3)
                                    if year_to_input and year_to:
                                        await year_to_input.fill(year_to)
                                        print(f"[cyberleninka] STEP 3: filled date_to field: {year_to}", flush=True)
                                        await asyncio.sleep(0.3)
                                    
                                    if (year_from_input and year_from) or (year_to_input and year_to):
                                        date_filter_applied = True
                                        
                                        # ШАГ 3b: Нажимаем кнопку "Задать" для применения фильтра по датам
                                        # Селектор: #body > div.content > div > div.search > div.infoblock.filter-block > div > div:nth-child(1) > ul > li.active > button
                                        zadat_btn_selectors = [
                                            "#body > div.content > div > div.search > div.infoblock.filter-block > div > div:nth-child(1) > ul > li.active > button",
                                            "div.infoblock.filter-block div > div:nth-child(1) > ul > li.active > button",
                                            "div.infoblock.filter-block button:has-text('Задать')",
                                            "div.infoblock.filter-block input[type='submit'][value*='Задать']",
                                            "div.infoblock.filter-block a:has-text('Задать')",
                                            ".filter-block button:has-text('Задать')",
                                            ".filter-block input[type='submit'][value*='Задать']",
                                            "div.search .infoblock button:has-text('Задать')",
                                        ]
                                        zadat_btn = None
                                        for sel in zadat_btn_selectors:
                                            try:
                                                zadat_btn = await page.query_selector(sel)
                                                if zadat_btn:
                                                    break
                                            except Exception:
                                                continue
                                        
                                        if zadat_btn:
                                            await zadat_btn.click()
                                            print(f"[cyberleninka] STEP 3b: clicked 'Задать' button to apply date filter", flush=True)
                                            await asyncio.sleep(2.0)  # Ждём применения фильтра и загрузки результатов
                                        else:
                                            print(f"[cyberleninka] WARNING: 'Задать' button not found for date filter", flush=True)
                            except Exception as e:
                                print(f"[cyberleninka] failed to set date filter: {e}", flush=True)
                        
                        # Если дат нет, нажимаем кнопку поиска после ввода keywords
                        if not date_filter_applied:
                            search_btn_after_keywords = await page.query_selector(search_btn_selector)
                            if search_btn_after_keywords:
                                await search_btn_after_keywords.click()
                                print(f"[cyberleninka] STEP 3: clicked search button after entering keywords (no date filter)", flush=True)
                                await asyncio.sleep(2.5)  # Ждём загрузки результатов
                        
                        # После нажатия "Задать" (или кнопки поиска, если дат нет) сразу парсим результаты
                        await asyncio.sleep(1.0)  # Дополнительное ожидание для загрузки результатов
                        # Блок с фильтром/результатами (как вы указали)
                        block_sel = (
                            "div.infoblock.filter-block, "
                            "div.search div.infoblock, "
                            "#body .content .search .infoblock.filter-block, "
                            "div.search a[href*='/article/n/']"
                        )
                        try:
                            await page.wait_for_selector(block_sel, timeout=8000)
                        except Exception:
                            pass
                        await asyncio.sleep(0.8)
                        
                        # Пагинация: собираем ссылки со всех страниц до достижения max_results
                        page_num = 1
                        seen_urls = set()
                        links = []
                        while len(links) < max_results:
                            content = await page.content()
                            page_links = self._parse_search_page(content, self.base_url)
                            if not page_links:
                                page_links = self._parse_search_page_regex(content, self.base_url)
                            # Дополнительно: ссылки из блока фильтра (ваш селектор)
                            for a in await page.query_selector_all("div.infoblock.filter-block a[href*='/article/'], div.search a[href*='/article/n/']"):
                                href = await a.get_attribute("href")
                                if href and "article/c/" not in href:
                                    full = urljoin(self.base_url, href).rstrip("/")
                                    if full not in seen_urls:
                                        seen_urls.add(full)
                                        title = await a.inner_text()
                                        page_links.append((title.strip() or "Статья", full))
                            
                            # Добавляем ссылки с текущей страницы
                            page_links_count = 0
                            for title, url in page_links:
                                if url not in seen_urls:
                                    seen_urls.add(url)
                                    links.append((title, url))
                                    page_links_count += 1
                                    if len(links) >= max_results:
                                        break
                            
                            print(f"[cyberleninka] page {page_num}: found {page_links_count} links, total: {len(links)}/{max_results}", flush=True)
                            
                            # Если собрали достаточно ссылок или на странице меньше 10 ссылок (конец результатов)
                            if len(links) >= max_results or page_links_count < 10:
                                break
                            
                            # Переходим на следующую страницу через кнопку пагинации (чтобы сохранить фильтр по датам)
                            page_num += 1
                            next_page_found = False
                            
                            # Ждём загрузки JavaScript и появления пагинации
                            await asyncio.sleep(2.0)  # Даём время на загрузку JavaScript
                            
                            # Прокручиваем страницу вниз, чтобы пагинация загрузилась (если она внизу)
                            try:
                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await asyncio.sleep(1.5)
                            except Exception:
                                pass
                            
                            # Пытаемся найти пагинацию через JavaScript (она может быть создана динамически)
                            try:
                                # Ищем все ссылки, которые содержат page= в href через JavaScript
                                pagination_hrefs = await page.evaluate("""
                                    () => {
                                        const links = Array.from(document.querySelectorAll('a[href*="page="], a[href*="/search?"]'));
                                        return links.map(link => ({
                                            href: link.href,
                                            text: link.innerText.trim(),
                                            visible: link.offsetParent !== null
                                        })).filter(link => link.visible && link.href.includes('page='));
                                    }
                                """)
                                print(f"[cyberleninka] found {len(pagination_hrefs)} pagination links via JavaScript", flush=True)
                                
                                # Ищем ссылку на нужную страницу
                                for link_info in pagination_hrefs:
                                    href = link_info.get('href', '')
                                    text = link_info.get('text', '')
                                    if href and (f"page={page_num}" in href.lower() or 
                                                (text == str(page_num) and "page=" in href.lower())):
                                        # Кликаем через JavaScript
                                        clicked = await page.evaluate(f"""
                                            () => {{
                                                const link = Array.from(document.querySelectorAll('a[href*="page="]')).find(a => 
                                                    a.href.includes('page={page_num}') || 
                                                    (a.innerText.trim() === '{page_num}' && a.href.includes('page='))
                                                );
                                                if (link) {{
                                                    link.click();
                                                    return true;
                                                }}
                                                return false;
                                            }}
                                        """)
                                        if clicked:
                                            print(f"[cyberleninka] clicked pagination link to page {page_num} via JavaScript (href: {href[:100]})", flush=True)
                                            next_page_found = True
                                            await asyncio.sleep(2.5)
                                            try:
                                                await page.wait_for_selector(block_sel, timeout=5000)
                                            except Exception:
                                                pass
                                            break
                            except Exception as e:
                                print(f"[cyberleninka] error finding pagination via JavaScript: {e}", flush=True)
                            
                            # Если не нашли через JavaScript, пробуем обычный поиск
                            if not next_page_found:
                                try:
                                    # Расширенный поиск всех возможных элементов пагинации
                                    pagination_links = await page.query_selector_all(
                                        ".pagination a, "
                                        ".pager a, "
                                        ".pages a, "
                                        "div.pagination a, "
                                        "ul.pagination a, "
                                        "ul.pager a, "
                                        "nav.pagination a, "
                                        "nav.pager a, "
                                        ".pagination li a, "
                                        ".pager li a, "
                                        "a[href*='page='], "
                                        "a[href*='/search?'], "
                                        "a[href*='&page='], "
                                        "a[href*='?page='], "
                                        "div.search a[href*='page='], "
                                        ".content a[href*='page=']"
                                    )
                                    print(f"[cyberleninka] found {len(pagination_links)} potential pagination links via selectors", flush=True)
                                    
                                    for link in pagination_links:
                                        try:
                                            href = await link.get_attribute("href")
                                            text = await link.inner_text()
                                            is_visible = await link.is_visible()
                                            
                                            if href and is_visible:
                                                # Проверяем разные варианты href для следующей страницы
                                                href_lower = href.lower()
                                                if (f"page={page_num}" in href_lower or 
                                                    f"&page={page_num}" in href_lower or
                                                    f"?page={page_num}" in href_lower or
                                                    (text.strip() == str(page_num) and ("page=" in href_lower or "/search" in href_lower))):
                                                    await link.scroll_into_view_if_needed()
                                                    await asyncio.sleep(0.3)
                                                    await link.click()
                                                    print(f"[cyberleninka] clicked pagination link to page {page_num} (href: {href[:100]}, text: {text.strip()})", flush=True)
                                                    next_page_found = True
                                                    await asyncio.sleep(2.5)  # Ждём загрузки следующей страницы
                                                    try:
                                                        await page.wait_for_selector(block_sel, timeout=5000)
                                                    except Exception:
                                                        pass
                                                    break
                                        except Exception as e:
                                            continue
                                except Exception as e:
                                    print(f"[cyberleninka] error finding pagination links via selectors: {e}", flush=True)
                                    import traceback
                                    traceback.print_exc()
                            
                            # Если не нашли по href, ищем по тексту
                            if not next_page_found:
                                next_page_selectors = [
                                    f"a:has-text('{page_num}')",
                                    "a:has-text('Следующая')",
                                    "a:has-text('Далее')",
                                    "a:has-text('→')",
                                    "a:has-text('>')",
                                    ".pagination a:has-text('→')",
                                    ".pagination a:has-text('>')",
                                    ".pager a:has-text('→')",
                                    ".pager a:has-text('>')",
                                ]
                                for sel in next_page_selectors:
                                    try:
                                        next_link = await page.query_selector(sel)
                                        if next_link:
                                            # Проверяем, что это действительно ссылка на следующую страницу
                                            href = await next_link.get_attribute("href")
                                            if href and ("page=" in href or "/search" in href):
                                                await next_link.scroll_into_view_if_needed()
                                                await asyncio.sleep(0.3)
                                                await next_link.click()
                                                print(f"[cyberleninka] clicked pagination link to page {page_num} (selector: {sel[:60]})", flush=True)
                                                next_page_found = True
                                                await asyncio.sleep(2.5)  # Ждём загрузки следующей страницы
                                                try:
                                                    await page.wait_for_selector(block_sel, timeout=5000)
                                                except Exception:
                                                    pass
                                                break
                                    except Exception:
                                        continue
                            
                            if not next_page_found:
                                # Сохраняем HTML для отладки структуры пагинации
                                try:
                                    debug_dir = Path("debug")
                                    debug_dir.mkdir(exist_ok=True)
                                    content_debug = await page.content()
                                    (debug_dir / f"cyberleninka_pagination_page_{page_num-1}.html").write_text(
                                        content_debug, encoding="utf-8"
                                    )
                                    print(f"[cyberleninka] saved HTML to debug/cyberleninka_pagination_page_{page_num-1}.html for debugging", flush=True)
                                except Exception:
                                    pass
                                
                                print(f"[cyberleninka] WARNING: pagination button for page {page_num} not found, using direct navigation with filter restoration", flush=True)
                                # Fallback: прямой переход с восстановлением фильтров
                                from urllib.parse import quote
                                try:
                                    next_page_url = f"{self.base_url}/search?q={quote(query)}&page={page_num}"
                                    await page.goto(
                                        next_page_url,
                                        wait_until="domcontentloaded",
                                        timeout=self.timeout * 1000,
                                    )
                                    await asyncio.sleep(2.0)  # Ждём загрузки страницы
                                    
                                    # Ждём появления элементов
                                    try:
                                        await page.wait_for_selector(block_sel, timeout=5000)
                                    except Exception:
                                        pass
                                    
                                    # Пытаемся восстановить фильтры по датам
                                    if date_from or date_to:
                                        await asyncio.sleep(2.0)  # Даём время на загрузку фильтров
                                        try:
                                            year_from = date_from[:4] if date_from and len(date_from) >= 4 else None
                                            year_to = date_to[:4] if date_to and len(date_to) >= 4 else None
                                            
                                            # Ждём появления блока фильтра с таймаутом
                                            filter_block = None
                                            try:
                                                await page.wait_for_selector(
                                                    "div.infoblock.filter-block, .filter-block, div.search .infoblock",
                                                    timeout=5000
                                                )
                                            except Exception:
                                                pass
                                            
                                            # Ищем блок фильтра (максимум 5 попыток)
                                            for attempt in range(5):
                                                filter_block = await page.query_selector(
                                                    "div.infoblock.filter-block, .filter-block, div.search .infoblock, "
                                                    "div.search div.infoblock, #body .content .search .infoblock.filter-block"
                                                )
                                                if filter_block:
                                                    break
                                                if attempt < 4:
                                                    await asyncio.sleep(0.8)
                                            
                                            if filter_block:
                                                # Ждём появления полей ввода
                                                await asyncio.sleep(1.0)
                                                
                                                # Ищем поля ввода дат - пробуем разные селекторы
                                                all_inputs = []
                                                selectors_list = [
                                                    "div.infoblock.filter-block input[type='text'], div.infoblock.filter-block input[type='number']",
                                                    ".filter-block input[type='text'], .filter-block input[type='number']",
                                                    "div.search .infoblock input[type='text'], div.search .infoblock input[type='number']",
                                                    "div.infoblock.filter-block input[placeholder*='от'], div.infoblock.filter-block input[placeholder*='до']",
                                                    ".filter-block input[placeholder*='от'], .filter-block input[placeholder*='до']",
                                                    "div.infoblock.filter-block ul input",
                                                    ".filter-block ul input",
                                                ]
                                                
                                                for selector in selectors_list:
                                                    all_inputs = await page.query_selector_all(selector)
                                                    if len(all_inputs) >= 2:
                                                        break
                                                
                                                # Если не нашли по селекторам, ищем все input в блоке фильтра
                                                if len(all_inputs) < 2:
                                                    all_inputs = await page.query_selector_all(
                                                        "div.infoblock.filter-block input, .filter-block input, div.search .infoblock input"
                                                    )
                                                
                                                print(f"[cyberleninka] found {len(all_inputs)} input fields in filter block", flush=True)
                                                
                                                if len(all_inputs) >= 2:
                                                    # Заполняем первое поле (от)
                                                    if year_from:
                                                        try:
                                                            await all_inputs[0].fill(year_from)
                                                            await asyncio.sleep(0.3)
                                                            print(f"[cyberleninka] filled date_from field: {year_from}", flush=True)
                                                        except Exception as e:
                                                            print(f"[cyberleninka] failed to fill date_from: {e}", flush=True)
                                                    
                                                    # Заполняем второе поле (до)
                                                    if year_to:
                                                        try:
                                                            await all_inputs[1].fill(year_to)
                                                            await asyncio.sleep(0.3)
                                                            print(f"[cyberleninka] filled date_to field: {year_to}", flush=True)
                                                        except Exception as e:
                                                            print(f"[cyberleninka] failed to fill date_to: {e}", flush=True)
                                                    
                                                    # Нажимаем "Задать"
                                                    zadat_btn = None
                                                    zadat_selectors = [
                                                        "div.infoblock.filter-block button:has-text('Задать')",
                                                        ".filter-block button:has-text('Задать')",
                                                        "div.infoblock.filter-block ul > li button:has-text('Задать')",
                                                        ".filter-block ul > li button:has-text('Задать')",
                                                        "div.infoblock.filter-block button span:has-text('Задать')",
                                                        "button:has-text('Задать')",
                                                    ]
                                                    
                                                    for sel in zadat_selectors:
                                                        try:
                                                            zadat_btn = await page.query_selector(sel)
                                                            if zadat_btn:
                                                                break
                                                        except Exception:
                                                            continue
                                                    
                                                    if zadat_btn:
                                                        await zadat_btn.click()
                                                        print(f"[cyberleninka] restored date filters after navigation to page {page_num}", flush=True)
                                                        await asyncio.sleep(4.0)  # Ждём применения фильтра и загрузки результатов
                                                        
                                                        # После применения фильтра страница возвращается на страницу 1
                                                        # Проверяем текущую страницу и используем кнопки пагинации для перехода на страницу 2
                                                        if page_num > 1:
                                                            print(f"[cyberleninka] filter applied, checking current page and navigating to page {page_num}", flush=True)
                                                            
                                                            # Проверяем текущую страницу через JavaScript
                                                            try:
                                                                current_page_text = await page.evaluate("""
                                                                    () => {
                                                                        const span = document.querySelector('h1.bigheader span');
                                                                        if (span) {
                                                                            const match = span.textContent.match(/страница\\s+(\\d+)/);
                                                                            return match ? parseInt(match[1]) : null;
                                                                        }
                                                                        return null;
                                                                    }
                                                                """)
                                                                print(f"[cyberleninka] current page detected: {current_page_text}", flush=True)
                                                            except Exception:
                                                                current_page_text = None
                                                            
                                                            # Если мы на странице 1, пытаемся использовать кнопки пагинации
                                                            if current_page_text == 1:
                                                                print(f"[cyberleninka] on page 1 after filter, trying to use pagination buttons to go to page {page_num}", flush=True)
                                                                
                                                                # Прокручиваем вниз, чтобы кнопки пагинации были видны
                                                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                                                await asyncio.sleep(1.0)
                                                                
                                                                # Ищем кнопки пагинации через JavaScript
                                                                pagination_clicked = False
                                                                try:
                                                                    pagination_clicked = await page.evaluate(f"""
                                                                        () => {{
                                                                            // Ищем ссылку на нужную страницу
                                                                            const links = Array.from(document.querySelectorAll('a[href*="page={page_num}"]'));
                                                                            for (const link of links) {{
                                                                                const href = link.getAttribute('href');
                                                                                if (href && href.includes('page={page_num}')) {{
                                                                                    link.click();
                                                                                    return true;
                                                                                }}
                                                                            }}
                                                                            // Ищем кнопку "Следующая" или "→"
                                                                            const nextButtons = Array.from(document.querySelectorAll('a')).filter(a => {{
                                                                                const text = a.textContent.trim();
                                                                                return text === 'Следующая' || text === '→' || text === '>' || text === '{page_num}';
                                                                            }});
                                                                            if (nextButtons.length > 0) {{
                                                                                nextButtons[0].click();
                                                                                return true;
                                                                            }}
                                                                            return false;
                                                                        }}
                                                                    """)
                                                                    if pagination_clicked:
                                                                        print(f"[cyberleninka] clicked pagination button via JavaScript to go to page {page_num}", flush=True)
                                                                        await asyncio.sleep(3.0)
                                                                        await page.wait_for_selector("ul.list, ul#search-results, div.search ul", timeout=8000)
                                                                except Exception as e:
                                                                    print(f"[cyberleninka] failed to click pagination via JavaScript: {e}", flush=True)
                                                                
                                                                # Если кнопки пагинации не сработали, используем прямой переход
                                                                if not pagination_clicked:
                                                                    print(f"[cyberleninka] pagination buttons not found, using direct URL navigation", flush=True)
                                                                    from urllib.parse import quote
                                                                    target_page_url = f"{self.base_url}/search?q={quote(query)}&page={page_num}"
                                                                    try:
                                                                        await page.goto(
                                                                            target_page_url,
                                                                            wait_until="domcontentloaded",
                                                                            timeout=self.timeout * 1000,
                                                                        )
                                                                        await asyncio.sleep(2.0)
                                                                        
                                                                        # Снова восстанавливаем фильтры (они могли сброситься при переходе)
                                                                        if date_from or date_to:
                                                                            await asyncio.sleep(1.0)
                                                                            try:
                                                                                year_from = date_from[:4] if date_from and len(date_from) >= 4 else None
                                                                                year_to = date_to[:4] if date_to and len(date_to) >= 4 else None
                                                                                
                                                                                filter_block = await page.query_selector(
                                                                                    "div.infoblock.filter-block, .filter-block, div.search .infoblock"
                                                                                )
                                                                                if filter_block:
                                                                                    all_inputs = await page.query_selector_all(
                                                                                        "div.infoblock.filter-block input, .filter-block input, div.search .infoblock input"
                                                                                    )
                                                                                    if len(all_inputs) >= 2:
                                                                                        if year_from:
                                                                                            await all_inputs[0].fill(year_from)
                                                                                        if year_to:
                                                                                            await all_inputs[1].fill(year_to)
                                                                                        
                                                                                        zadat_btn2 = await page.query_selector(
                                                                                            "div.infoblock.filter-block button:has-text('Задать'), .filter-block button:has-text('Задать')"
                                                                                        )
                                                                                        if zadat_btn2:
                                                                                            await zadat_btn2.click()
                                                                                            print(f"[cyberleninka] re-applied date filters on page {page_num}", flush=True)
                                                                                            await asyncio.sleep(3.0)
                                                                            except Exception as e:
                                                                                print(f"[cyberleninka] failed to re-apply filters: {e}", flush=True)
                                                                        
                                                                        # Ждём появления результатов
                                                                        try:
                                                                            await page.wait_for_selector("ul.list, ul#search-results, div.search ul", timeout=8000)
                                                                        except Exception:
                                                                            pass
                                                                        
                                                                        await asyncio.sleep(1.0)
                                                                    except Exception as e:
                                                                        print(f"[cyberleninka] failed to navigate back to page {page_num}: {e}", flush=True)
                                                        
                                                        # Прокручиваем страницу несколько раз для загрузки всех элементов
                                                        for scroll_attempt in range(3):
                                                            try:
                                                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                                                await asyncio.sleep(0.8)
                                                                await page.evaluate("window.scrollTo(0, 0)")
                                                                await asyncio.sleep(0.5)
                                                            except Exception:
                                                                pass
                                                        
                                                        # После восстановления фильтров заново получаем контент и парсим ссылки
                                                        print(f"[cyberleninka] re-parsing page {page_num} after filter restoration", flush=True)
                                                        content_after_filter = await page.content()
                                                        
                                                        # Сохраняем HTML для отладки
                                                        try:
                                                            debug_dir = Path("debug")
                                                            debug_dir.mkdir(exist_ok=True)
                                                            (debug_dir / f"cyberleninka_page_{page_num}_after_filter.html").write_text(
                                                                content_after_filter, encoding="utf-8"
                                                            )
                                                            print(f"[cyberleninka] saved HTML to debug/cyberleninka_page_{page_num}_after_filter.html", flush=True)
                                                        except Exception:
                                                            pass
                                                        
                                                        soup_after_filter = BeautifulSoup(content_after_filter, "html.parser")
                                                        
                                                        # Парсим ссылки с обновлённой страницы - пробуем разные селекторы
                                                        page_links_count_after = 0
                                                        
                                                        # Селектор 1: все ссылки на статьи
                                                        for a in soup_after_filter.select("a[href*='/article/n/']"):
                                                            href = a.get("href")
                                                            if not href or "article/c/" in href:
                                                                continue
                                                            full = urljoin(self.base_url, href).rstrip("/")
                                                            if full not in seen_urls:
                                                                seen_urls.add(full)
                                                                title = a.get_text(strip=True) or "Статья"
                                                                links.append((title, full))
                                                                page_links_count_after += 1
                                                                if len(links) >= max_results:
                                                                    break
                                                        
                                                        # Селектор 2: ссылки в списке результатов (ul.list li h2 a)
                                                        if page_links_count_after == 0:
                                                            for a in soup_after_filter.select("ul.list li h2 a[href*='/article/'], ul#search-results li h2 a[href*='/article/'], div.search ul li h2 a[href*='/article/']"):
                                                                if len(links) >= max_results:
                                                                    break
                                                                href = a.get("href")
                                                                if not href or "article/c/" in href:
                                                                    continue
                                                                full = urljoin(self.base_url, href).rstrip("/")
                                                                if full not in seen_urls:
                                                                    seen_urls.add(full)
                                                                    links.append((a.get_text(strip=True) or "Статья", full))
                                                                    page_links_count_after += 1
                                                        
                                                        # Селектор 3: ссылки в блоке фильтра и поиска
                                                        for a in soup_after_filter.select("div.infoblock.filter-block a[href*='/article/'], div.search a[href*='/article/']"):
                                                            if len(links) >= max_results:
                                                                break
                                                            href = a.get("href")
                                                            if not href or "article/c/" in href:
                                                                continue
                                                            full = urljoin(self.base_url, href).rstrip("/")
                                                            if full not in seen_urls:
                                                                seen_urls.add(full)
                                                                links.append((a.get_text(strip=True) or "Статья", full))
                                                                page_links_count_after += 1
                                                        
                                                        print(f"[cyberleninka] page {page_num} after filter restoration: found {page_links_count_after} links, total: {len(links)}/{max_results}", flush=True)
                                                        
                                                        # Если собрали достаточно ссылок, выходим из цикла
                                                        if len(links) >= max_results:
                                                            break
                                                        
                                                        # Если на странице меньше 10 ссылок, это может быть конец результатов
                                                        if page_links_count_after < 10:
                                                            print(f"[cyberleninka] page {page_num}: only {page_links_count_after} links found, stopping pagination", flush=True)
                                                            break
                                                        
                                                        # Переходим к следующей странице
                                                        page_num += 1
                                                        continue
                                                    else:
                                                        print(f"[cyberleninka] WARNING: 'Задать' button not found", flush=True)
                                                else:
                                                    print(f"[cyberleninka] WARNING: date input fields not found (found {len(all_inputs)} inputs)", flush=True)
                                            else:
                                                print(f"[cyberleninka] WARNING: filter block not found", flush=True)
                                        except Exception as e:
                                            print(f"[cyberleninka] failed to restore date filters: {e}", flush=True)
                                            import traceback
                                            traceback.print_exc()
                                    
                                    await asyncio.sleep(0.5)
                                    print(f"[cyberleninka] navigated to page {page_num}", flush=True)
                                    
                                    # После восстановления фильтров нужно заново получить контент страницы
                                    # Продолжаем цикл пагинации с обновлённым контентом
                                    continue
                                except Exception as e:
                                    print(f"[cyberleninka] failed to navigate to page {page_num}: {e}", flush=True)
                                    break
                    finally:
                        await browser.close()
                return links[:max_results] if links else []
            
            # Запускаем с таймаутом
            result_links = await asyncio.wait_for(
                _run_playwright(),
                timeout=self.timeout + 30  # Дополнительные 30 сек на обработку
            )
            return result_links
        except asyncio.TimeoutError:
            print(f"[cyberleninka] Playwright UI search timeout after {self.timeout + 30}s", flush=True)
            return []
        except Exception as e:
            print(f"[cyberleninka] Playwright UI search error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return []

    def _parse_search_page_regex(self, html: str, base: str) -> list[tuple[str, str]]:
        """Извлечение ссылок на статьи по regex (если разметка нестандартная или JS)."""
        seen: set[str] = set()
        links: list[tuple[str, str]] = []
        for pattern in (ARTICLE_LINK_RE, ARTICLE_LINK_ALT_RE, ARTICLE_REL_RE):
            for m in pattern.finditer(html):
                href = m.group(1).split("#")[0].split("?")[0].strip()
                if not href or "article/c/" in href:
                    continue
                full_url = urljoin(base, href)
                norm = full_url.rstrip("/")
                if norm in seen:
                    continue
                seen.add(norm)
                title = "Статья"
                if "/article/n/" in norm:
                    slug = norm.split("/article/n/")[-1]
                    if slug:
                        title = slug.replace("-", " ").strip()[:200]
                links.append((title, full_url.rstrip("/")))
        if not links and html:
            links = self._parse_article_links_aggressive(html, base)
        return links[:50]

    def _parse_article_links_aggressive(self, html: str, base: str) -> list[tuple[str, str]]:
        """Ищем /article/n/SLUG в любом месте HTML (JSON, data-*, минифицированный код)."""
        seen: set[str] = set()
        links: list[tuple[str, str]] = []
        for m in ARTICLE_ANY_RE.finditer(html):
            slug_part = m.group(1).strip()
            if "article/c/" in slug_part or not slug_part:
                continue
            if slug_part.startswith("article/n/"):
                path = "/" + slug_part
            else:
                path = "/" + slug_part if not slug_part.startswith("/") else slug_part
            full_url = urljoin(base, path).rstrip("/").split("?")[0].split("#")[0]
            if full_url in seen:
                continue
            seen.add(full_url)
            if "/article/n/" in full_url:
                slug = full_url.split("/article/n/")[-1]
                title = slug.replace("-", " ").strip()[:200] if slug else "Статья"
            else:
                title = "Статья"
            links.append((title, full_url))
        return links[:50]

    def _debug_save_search_page(self, url: str, html: str, html_js: str = "") -> None:
        """При 0 результатах сохраняем HTML в debug/ для разбора причины."""
        try:
            debug_dir = Path("debug")
            debug_dir.mkdir(exist_ok=True)
            (debug_dir / "cyberleninka_search_aiohttp.html").write_text(
                html or "(empty)", encoding="utf-8"
            )
            if html_js:
                (debug_dir / "cyberleninka_search_playwright.html").write_text(
                    html_js, encoding="utf-8"
                )
            (debug_dir / "cyberleninka_search_url.txt").write_text(url, encoding="utf-8")
        except Exception:
            pass

    def _parse_search_page(self, html: str, base: str) -> list[tuple[str, str]]:
        """Парсинг страницы поиска/категории: список (title, link). Селекторы из рабочих скриптов."""
        soup = BeautifulSoup(html, "html.parser")
        links: list[tuple[str, str]] = []
        seen: set[str] = set()

        # Как в скриптах: //div[@class='full']//a/@href — ссылки на статьи в блоке full
        full_div = soup.select_one("div.full")
        if full_div:
            for a in full_div.select("a[href*='/article/']"):
                href = a.get("href")
                if href and "article/c/" not in href:
                    full_url = urljoin(base, href).rstrip("/")
                    if full_url not in seen:
                        seen.add(full_url)
                        title = a.get_text(strip=True) or "Без названия"
                        links.append((title, full_url))

        for item in soup.select("article, .search-result-item, .serp-item, .item"):
            a = item.select_one("a[href*='/article/']") or item.select_one("a.title")
            if not a:
                a = item.select_one("a[href]")
            if a and a.get("href"):
                href = a["href"]
                if "/article/" in href or "/journal/" in href:
                    full_url = urljoin(base, href).rstrip("/")
                    if full_url not in seen:
                        seen.add(full_url)
                        title_el = item.select_one("h2, .title, .item__title, a")
                        title = (title_el.get_text(strip=True) if title_el else a.get_text(strip=True)) or "Без названия"
                        links.append((title, full_url))
        if not links:
            for a in soup.select('a[href*="/article/"]'):
                href = a.get("href")
                if href and "article/c/" not in href:
                    full_url = urljoin(base, href).rstrip("/")
                    if full_url not in seen:
                        seen.add(full_url)
                        links.append((a.get_text(strip=True) or "Без названия", full_url))
        if not links:
            links = self._parse_search_page_regex(html, base)
        if not links:
            links = self._parse_search_page_script_json(html, base)
        return links[:50]

    def _parse_search_page_script_json(self, html: str, base: str) -> list[tuple[str, str]]:
        """Извлечение ссылок на статьи из JSON в script-тегах (SPA)."""
        links: list[tuple[str, str]] = []
        seen: set[str] = set()
        for pattern in (r'/article/n/[^"\'?\s]+', r'"/article/n/[^"]+', r"href.*?/article/n/[^\"']+"):
            for m in re.finditer(pattern, html):
                raw = m.group(0)
                href = raw
                if href.startswith("href"):
                    href = re.sub(r'^href["\']?\s*[:=]\s*["\']?', "", href)
                href = href.strip('"\')\\]').split("?")[0].split("#")[0]
                if "/article/n/" not in href or "article/c/" in href:
                    continue
                if not href.startswith("http"):
                    href = urljoin(base, href)
                norm = href.rstrip("/")
                if norm in seen:
                    continue
                seen.add(norm)
                title = "Статья"
                if "/article/n/" in norm:
                    slug = norm.split("/article/n/")[-1]
                    if slug:
                        title = slug.replace("-", " ").strip()[:200]
                links.append((title, norm))
        return links[:50]

    # Маркеры конца основного текста статьи (как в рабочих скриптах)
    _END_ARTICLE_PHRASES = (
        "библиографический список",
        "библиографический список:",
        "список литературы",
        "список литературы:",
        "литература",
        "литература:",
    )
    # Строки/паттерны метаданных издателя (не текст статьи) — исключаем из full_text
    _PUBLISHER_JUNK_PATTERNS = (
        "URL статьи:",
        "Ссылка для цитирования",
        "https://mir-nauki.com",
        "Мир науки.",
        "World of Science.",
        "педагогика и психология",
        "https ://",
        "https://",
    )

    def _is_publisher_junk_paragraph(self, para: str) -> bool:
        """Параграф — технический мусор издателя (URL статьи, ссылка для цитирования и т.д.), не текст статьи."""
        if not para or len(para.strip()) < 10:
            return False
        p = para.strip()
        for pat in self._PUBLISHER_JUNK_PATTERNS:
            if pat in p:
                return True
        if p.count("https") >= 2 or (p.startswith("http") and p.count("/") >= 4 and len(p) < 200):
            return True
        return False

    def _clean_article_text(self, text: str) -> str:
        """Удаляет из текста строки с метаданными издателя (URL статьи, ссылки, шапки сайта)."""
        if not text or not text.strip():
            return text
        lines = text.split("\n")
        out = []
        for line in lines:
            s = line.strip()
            if not s:
                out.append(line)
                continue
            junk = False
            for pat in self._PUBLISHER_JUNK_PATTERNS:
                if pat in s:
                    junk = True
                    break
            if not junk and not (s.startswith("http") and s.count("/") >= 3 and len(s) < 300):
                out.append(line)
        return "\n".join(out).strip()

    def _parse_article_page(
        self, html: str, url: str, *, fallback_title: str | None = None
    ) -> SourceResult | None:
        """Парсинг страницы статьи. Приоритет — селекторы из рабочих скриптов (div.main, div.ocr, infoblock).
        fallback_title — заголовок со страницы поиска, используется если на странице статьи заголовок не найден."""
        try:
            return self._parse_article_page_impl(html, url, fallback_title=fallback_title)
        except Exception as e:
            _debug_log(f"_parse_article_page error: {url[:60]} -> {e}")
            print(f"[cyberleninka] _parse_article_page error for {url[:60]}...: {e}", flush=True)
            logger.debug("_parse_article_page error: %s", e, exc_info=True)
            return None

    def _parse_article_page_impl(
        self, html: str, url: str, *, fallback_title: str | None = None
    ) -> SourceResult | None:
        soup = BeautifulSoup(html, "html.parser")

        # --- Заголовок: //div[@class='main']//h1/i/text() или h1 ---
        title = ""
        main_div = soup.select_one("div.main")
        if main_div:
            h1 = main_div.select_one("h1 i") or main_div.select_one("h1")
            if h1:
                title = h1.get_text(strip=True)
        if not title:
            for sel in ("h1", ".article__title", ".title", "[class*='title']", "[itemprop='headline']", "h2"):
                el = soup.select_one(sel)
                if el:
                    title = el.get_text(strip=True)
                    if title and len(title) > 2 and len(title) < 500:
                        break
        if not title:
            meta = soup.select_one('meta[property="og:title"], meta[name="citation_title"]')
            if meta and meta.get("content"):
                title = meta["content"].strip()
        if not title:
            title_tag = soup.find("title")
            if title_tag and title_tag.get_text(strip=True):
                t = title_tag.get_text(strip=True)
                for suffix in (" — КиберЛенинка", " - КиберЛенинка", " | КиберЛенинка"):
                    if t.endswith(suffix):
                        t = t[: -len(suffix)].strip()
                        break
                if len(t) > 3:
                    title = t
        # JSON-LD (schema.org Article)
        if not title and isinstance(html, str):
            try:
                for script in soup.select('script[type="application/ld+json"]'):
                    raw = script.string
                    if not raw:
                        continue
                    data = json.loads(raw)
                    if isinstance(data, dict):
                        name = data.get("name") or data.get("headline")
                        if isinstance(name, str) and 3 < len(name) < 500:
                            title = name.strip()
                            break
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                name = item.get("name") or item.get("headline")
                                if isinstance(name, str) and 3 < len(name) < 500:
                                    title = name.strip()
                                    break
                        if title:
                            break
            except Exception:
                pass

        # --- Авторы: //div[@class='infoblock authors visible']/ul[@class='author-list']/li[@itemprop='author']/span ---
        # Исключаем строки-лицензии вида "CC BY5002522" и чисто цифровые
        def _is_author_name(s: str) -> bool:
            if not s or len(s) > 200:
                return False
            s_upper = s.strip().upper()
            if s_upper.startswith("CC BY") or re.match(r"^[\d\s]+$", s):
                return False
            return True

        authors: list[str] = []
        infoblock_authors = soup.select_one("div.infoblock.authors.visible ul.author-list")
        if infoblock_authors:
            for li in infoblock_authors.select("li[itemprop='author'] span"):
                t = li.get_text(strip=True)
                if _is_author_name(t):
                    authors.append(t)
        if not authors:
            for el in soup.select(".author, .article__author, [class*='author']"):
                t = el.get_text(strip=True)
                if _is_author_name(t):
                    authors.append(t)

        # --- DOI (для выхода по образцу: "10.1234/example") ---
        doi_val = None
        try:
            meta_doi = soup.select_one('meta[name="citation_doi"], meta[name="DOI"]')
            if meta_doi and meta_doi.get("content"):
                doi_val = meta_doi["content"].strip()
            if not doi_val and isinstance(html, str):
                doi_match = re.search(r"\b(10\.\d{4,}/[^\s\"'<>]+)", html)
                if doi_match:
                    doi_val = doi_match.group(1).rstrip(".,;:)")
        except Exception:
            doi_val = None
        if doi_val:
            doi_val = doi_val.strip().rstrip("/") or None

        # --- Аннотация: meta citation_abstract, затем 4-й infoblock (как раньше), затем остальные ---
        abstract = ""
        try:
            meta_abstract = soup.select_one('meta[name="citation_abstract"]')
            if meta_abstract and meta_abstract.get("content"):
                abstract = meta_abstract["content"].strip()
            if not abstract:
                infoblocks = soup.select("div.infoblock")
                if len(infoblocks) >= 4:
                    fourth = infoblocks[3]
                    abstract_div = fourth.select_one("div.full.abstract p[itemprop='description']")
                    if abstract_div:
                        abstract = abstract_div.get_text(separator=" ", strip=True)
                    else:
                        for p in fourth.select("div.abstract p, div.full p"):
                            abstract = p.get_text(separator=" ", strip=True)
                            if len(abstract) > 20:
                                break
                if not abstract:
                    for ib in infoblocks:
                        abstract_div = ib.select_one("div.full.abstract p[itemprop='description']")
                        if abstract_div:
                            abstract = abstract_div.get_text(separator=" ", strip=True)
                            break
                        for p in ib.select("div.abstract p, div.full.abstract p, div.full p"):
                            t = p.get_text(separator=" ", strip=True)
                            if len(t) > 50:
                                abstract = t
                                break
                        if abstract:
                            break
            if not abstract:
                for sel in (".abstract", ".article__abstract", ".annote", "[class*='abstract']", "p[itemprop='description']"):
                    el = soup.select_one(sel)
                    if el:
                        abstract = el.get_text(separator=" ", strip=True)
                        if len(abstract) > 20:
                            break
            if not abstract:
                meta = soup.select_one('meta[name="description"], meta[property="og:description"]')
                if meta and meta.get("content"):
                    abstract = meta["content"].strip()
        except Exception:
            abstract = ""

        # --- Основной текст: только div.main div.ocr (основной контент, без сайдбара/вставок издателя) ---
        full_text = ""
        ocr_div = main_div.select_one("div.ocr") if main_div else soup.select_one("div.ocr")
        _first_paragraphs: list[str] = []  # для fallback аннотации
        if ocr_div:
            paragraphs = ocr_div.select("p")
            text_list = [p.get_text(strip=True).replace("\ufeff", "") for p in paragraphs]
            title_lower = (title or "").lower()
            idx = 0
            while idx < len(text_list):
                para = text_list[idx]
                if self._is_publisher_junk_paragraph(para):
                    idx += 1
                    continue
                if len(para) > 30 and len(_first_paragraphs) < 2:
                    _first_paragraphs.append(para)
                if title_lower and title_lower in para.lower() and idx < len(text_list) - 1:
                    idx += 1
                    while idx < len(text_list):
                        p2 = text_list[idx]
                        if p2.lower().strip() in self._END_ARTICLE_PHRASES:
                            break
                        if not self._is_publisher_junk_paragraph(p2):
                            full_text += p2 + " "
                        idx += 1
                    break
                elif not title_lower and para.strip():
                    full_text += para + " "
                idx += 1
            full_text = full_text.strip()
            # Если по заголовку текст не нашли — берём все абзацы (вёрстка без повтора заголовка в тексте)
            if not full_text and text_list:
                full_text = " ".join(p for p in text_list if p.strip() and not self._is_publisher_junk_paragraph(p)).strip()
        if not abstract and _first_paragraphs:
            abstract = " ".join(_first_paragraphs)[:5000].strip()
        if not full_text:
            full_text_parts = []
            scope = main_div if main_div else soup
            for block in scope.select(".article__body, .fulltext, .body, .text, [class*='body']"):
                full_text_parts.append(block.get_text(separator="\n", strip=True))
            full_text = "\n\n".join(full_text_parts) if full_text_parts else ""
        full_text = self._clean_article_text(full_text)

        # Ключевые слова: //div[@class='full keywords']/i[@itemprop='keywords']/span/text()
        keywords_list: list[str] = []
        kw_div = soup.select_one("div.full.keywords") or soup.select_one("div[class*='keywords']")
        if kw_div:
            for span in kw_div.select("i[itemprop='keywords'] span"):
                t = span.get_text(strip=True)
                if t:
                    keywords_list.append(t)
            if not keywords_list:
                for span in kw_div.select("span"):
                    t = span.get_text(strip=True)
                    if t:
                        keywords_list.append(t)

        pdf_url = None
        for a in soup.select('a[href*=".pdf"], a[href*="/pdf/"], a[download]'):
            href = a.get("href")
            if href and ".pdf" in href.lower():
                pdf_url = urljoin(url, href)
                break
        if not pdf_url:
            for a in soup.find_all("a", href=True):
                if "pdf" in a.get("href", "").lower() or "PDF" in (a.get_text() or ""):
                    pdf_url = urljoin(url, a["href"])
                    break

        date_val = None
        for el in soup.select("[class*='date'], time"):
            if el.get("datetime"):
                date_val = el["datetime"][:10]
                break
            t = el.get_text(strip=True)
            if t and re.match(r"\d{4}-\d{2}-\d{2}|\d{4}", t):
                date_val = t[:10] if "-" in t else f"{t}-01-01"
                break

        metadata: dict = {}
        if keywords_list:
            metadata["keywords"] = keywords_list

        # Не сохраняем только явную CAPTCHA; страницы с заголовком и хоть каким-то текстом — оставляем
        title_clean = (title or "").strip()
        if not title_clean and fallback_title and fallback_title.strip():
            fallback = fallback_title.strip()
            # Допускаем длинный заголовок со страницы поиска (до 2000), иначе часть статей отбрасывалась
            if 2 < len(fallback) < 2000:
                captcha_titles = ("вы точно человек", "are you human", "подтвердите", "captcha", "robot")
                if not any(c in fallback.lower() for c in captcha_titles):
                    title_clean = fallback[:500] if len(fallback) > 500 else fallback
                    logger.debug("cyberleninka: using fallback_title from search page: %s", url[:60])
        # Последний fallback: заголовок из URL (slug после /article/n/)
        if not title_clean and "/article/n/" in url:
            slug = url.split("/article/n/")[-1].split("?")[0].split("#")[0].strip()
            if slug and len(slug) > 2:
                slug = unquote(slug)
                title_from_slug = slug.replace("-", " ").strip()[:300]
                if len(title_from_slug) >= 3:
                    title_clean = title_from_slug
                    logger.debug("cyberleninka: using title from URL slug: %s", url[:60])
        captcha_titles = ("вы точно человек", "are you human", "подтвердите", "captcha", "robot")
        if title_clean and (
            title_clean.lower() in captcha_titles or any(c in title_clean.lower() for c in captcha_titles)
        ):
            return None
        if not title_clean:
            _debug_log(f"article page no title (url ok): {url[:70]}")
            logger.debug("cyberleninka: article parsed None (no title): %s", url[:80])
            return None
        # Минимум контента: текст или аннотация; если нет — оставляем хотя бы заголовок (чтобы не терять документ)
        full_text_use = (full_text or abstract or title_clean or "").strip()

        return SourceResult(
            title=title_clean or "Без названия",
            url=url,
            authors=authors,
            date=date_val,
            doi=doi_val,
            abstract=abstract,
            full_text=full_text or abstract or title_clean,
            pdf_url=pdf_url,
            metadata=metadata,
            source_name=self.name,
        )

    def _parse_category_links(self, html: str, base: str) -> list[tuple[str, str]]:
        """Ссылки на разделы с главной: div.half ul.grnti a; при отсутствии — любые a[href*='/article/c/']."""
        soup = BeautifulSoup(html, "html.parser")
        links = []
        seen: set[str] = set()
        for a in soup.select("div.half ul.grnti a, div.half-right ul.grnti a"):
            href = a.get("href")
            if href and href.strip() and "/article/c/" in href:
                full_url = urljoin(base, href).rstrip("/")
                if full_url not in seen:
                    seen.add(full_url)
                    links.append((a.get_text(strip=True) or "Category", full_url))
        if not links:
            for a in soup.select('a[href*="/article/c/"]'):
                href = a.get("href")
                if href and href.strip():
                    full_url = urljoin(base, href).rstrip("/")
                    if full_url not in seen:
                        seen.add(full_url)
                        links.append((a.get_text(strip=True) or "Category", full_url))
        return links

    async def _search_via_categories(
        self,
        session: aiohttp.ClientSession,
        max_results: int,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[SourceResult]:
        """Запасной способ: главная -> категории -> div.full статьи (без JS). Если категорий нет — ищем статьи на главной."""
        results = []
        category_links: list[tuple[str, str]] = []
        try:
            main_html = await self._fetch(session, self.base_url + "/")
            category_links = self._parse_category_links(main_html, self.base_url)
            if not category_links:
                for div in BeautifulSoup(main_html, "html.parser").select("div.half, div.half-right"):
                    for a in div.select("ul a[href*='article']"):
                        href = a.get("href")
                        if href:
                            category_links.append((a.get_text(strip=True) or "Cat", urljoin(self.base_url, href)))
            if not category_links:
                category_links = [
                    (p.split("/")[-1].replace("-", " "), f"{self.base_url}/{p}")
                    for p in FALLBACK_CATEGORY_PATHS
                ]
            _debug_log(f"cyberleninka category_links={len(category_links)}")
            print(f"[cyberleninka] category_links={len(category_links)}", flush=True)
            logger.info("cyberleninka: category_links=%s", len(category_links))
            # Если категорий так и нет — пробуем вытащить ссылки на статьи прямо с главной (regex/любые /article/n/)
            if not category_links and main_html:
                direct_article_links = self._parse_search_page(main_html, self.base_url)
                if not direct_article_links:
                    direct_article_links = self._parse_article_links_aggressive(main_html, self.base_url)
                if direct_article_links:
                    print(f"[cyberleninka] using {len(direct_article_links)} article links from main page", flush=True)
                    logger.info("cyberleninka: using %s article links from main page", len(direct_article_links))
                    seen_urls = set()
                    for _title, art_url in direct_article_links[:max_results]:
                        if art_url in seen_urls:
                            continue
                        seen_urls.add(art_url)
                        try:
                            art_html = await self._fetch_article_html(session, art_url)
                            art = self._parse_article_page(art_html, art_url)
                            if art and self._date_in_range(art.date, date_from, date_to):
                                results.append(art)
                                if len(results) >= max_results:
                                    return results[:max_results]
                        except Exception:
                            continue
                    return results[:max_results]
            seen_urls = set()
            for _cat_name, cat_url in category_links[:8]:
                if len(results) >= max_results:
                    break
                try:
                    cat_html = await self._fetch(session, cat_url)
                    soup = BeautifulSoup(cat_html, "html.parser")
                    article_links: list[str] = []
                    full_div = soup.select_one("div.full")
                    if full_div:
                        for a in full_div.select("a[href*='/article/']"):
                            href = a.get("href")
                            if not href or "article/c/" in href:
                                continue
                            full_url = urljoin(self.base_url, href).rstrip("/")
                            if "/article/n/" in full_url or "/article/" in full_url:
                                article_links.append(full_url)
                    if not article_links:
                        for a in soup.select('a[href*="/article/n/"]'):
                            href = a.get("href")
                            if href:
                                full_url = urljoin(self.base_url, href).rstrip("/").split("?")[0].split("#")[0]
                                article_links.append(full_url)
                    _debug_log(f"cyberleninka category_page {cat_url[-50:]} article_links={len(article_links)}")
                    for full_url in article_links:
                        if full_url in seen_urls:
                            continue
                        seen_urls.add(full_url)
                        try:
                            art_html = await self._fetch_article_html(session, full_url)
                            art = self._parse_article_page(art_html, full_url)
                            if art and self._date_in_range(art.date, date_from, date_to):
                                results.append(art)
                                if len(results) >= max_results:
                                    break
                            else:
                                _debug_log(f"article parsed None: {full_url[:70]}")
                        except Exception as ex:
                            _debug_log(f"article fetch/parse error: {full_url[:50]} {ex}")
                            continue
                except Exception:
                    continue
        except Exception as e:
            _debug_log(f"cyberleninka _search_via_categories error: {e}")
            print(f"[cyberleninka] _search_via_categories error: {e}", flush=True)
            logger.warning("cyberleninka: _search_via_categories error: %s", e)
        if not results:
            _debug_log("cyberleninka trying FALLBACK_ARTICLE_PATHS (0 results so far)")
            for path in FALLBACK_ARTICLE_PATHS:
                if len(results) >= max_results:
                    break
                art_url = f"{self.base_url}/{path}"
                try:
                    art_html = await self._fetch_article_html(session, art_url)
                    art = self._parse_article_page(art_html, art_url)
                    if art and self._date_in_range(art.date, date_from, date_to):
                        results.append(art)
                        _debug_log(f"cyberleninka fallback article ok: {path[:50]}")
                except Exception as e:
                    _debug_log(f"cyberleninka fallback article fail {path[:30]}: {e}")
        _debug_log(f"cyberleninka _search_via_categories returned {len(results)} results")
        print(f"[cyberleninka] _search_via_categories returned {len(results)} results", flush=True)
        logger.info("cyberleninka: _search_via_categories returned %s results", len(results))
        return results[:max_results]

    async def search(
        self,
        query: str,
        *,
        max_results: int = 50,
        date_from: str | None = None,
        date_to: str | None = None,
        languages: list[str] | None = None,
    ) -> list[SourceResult]:
        """Поиск по ключевым словам на CyberLeninka. При 0 по поиску — обход по категориям."""
        _debug_log("=== cyberleninka search start ===")
        results: list[SourceResult] = []
        page = 1
        
        # Если указаны даты, ВСЕГДА используем Playwright UI для применения фильтра по датам
        # Обычный поиск не поддерживает фильтр по датам через URL параметры
        use_playwright_for_dates = (date_from or date_to) and self.use_playwright
        
        # Если нужен фильтр по датам, сразу используем Playwright UI, пропуская обычный поиск
        if use_playwright_for_dates:
            print(f"[cyberleninka] date filter specified (date_from={date_from}, date_to={date_to}), using Playwright UI", flush=True)
            async with aiohttp.ClientSession() as session:
                if self.use_playwright:
                    links_ui = await self._search_via_playwright_ui(query, max_results, date_from, date_to)
                    print(f"[cyberleninka] collected {len(links_ui)} links from Playwright UI, max_results={max_results}", flush=True)
                    if links_ui:
                        processed_count = 0
                        successful_count = 0
                        failed_count = 0
                        for i, (title, article_url) in enumerate(links_ui, 1):
                            if len(results) >= max_results:
                                print(f"[cyberleninka] reached max_results={max_results}, stopping. Processed: {processed_count}, successful: {successful_count}, failed: {failed_count}", flush=True)
                                break
                            processed_count += 1
                            try:
                                art_html = await self._fetch_article_html(session, article_url)
                                art = self._parse_article_page(art_html, article_url, fallback_title=title)
                                if art:
                                    if self._date_in_range(art.date, date_from, date_to):
                                        results.append(art)
                                        successful_count += 1
                                        print(f"[cyberleninka] article {i}/{len(links_ui)}: parsed successfully, date={art.date}", flush=True)
                                    else:
                                        failed_count += 1
                                        print(f"[cyberleninka] article {i}/{len(links_ui)}: date {art.date} out of range ({date_from} - {date_to})", flush=True)
                                else:
                                    failed_count += 1
                                    print(f"[cyberleninka] article {i}/{len(links_ui)}: parsed None (no title/structure)", flush=True)
                            except Exception as e:
                                failed_count += 1
                                print(f"[cyberleninka] article {i}/{len(links_ui)}: error: {e}", flush=True)
                                continue
                        print(f"[cyberleninka] final: processed {processed_count}/{len(links_ui)} links, successful: {successful_count}, failed: {failed_count}, results: {len(results)}", flush=True)
                return results[:max_results]
        
        # Обычный поиск без фильтра по датам (или если Playwright недоступен)
        async with aiohttp.ClientSession() as session:
            while len(results) < max_results:
                url = self._search_url(query, page=page)
                try:
                    html = await self._fetch(session, url)
                except Exception:
                    break
                links = self._parse_search_page(html, self.base_url)
                if not links and html:
                    links = self._parse_search_page_regex(html, self.base_url)
                if not links and html:
                    links = self._parse_search_page_script_json(html, self.base_url)
                html_js = ""
                if self.use_playwright and not links and page == 1:
                    html_js = await self._fetch_with_playwright(url)
                    if html_js:
                        links = self._parse_search_page(html_js, self.base_url)
                        if not links:
                            links = self._parse_search_page_regex(html_js, self.base_url)
                _debug_log(f"cyberleninka search_page links={len(links)} query={query[:40] if query else ''}")
                print(f"[cyberleninka] search page links={len(links)} (query={query[:50] if query else ''})", flush=True)
                logger.info("cyberleninka: search page links=%s (query=%s)", len(links), query[:50] if query else "")
                
                if not links and page == 1:
                    self._debug_save_search_page(url, html, html_js)
                if not links and page == 1:
                    print("[cyberleninka] fallback to categories (or playwright UI)", flush=True)
                    logger.info("cyberleninka: fallback to categories (or playwright UI)")
                    if self.use_playwright:
                        # Поиск через UI: главная -> ввод keywords в строку -> установка фильтра по датам -> блок фильтра
                        links_ui = await self._search_via_playwright_ui(query, max_results, date_from, date_to)
                        if links_ui:
                            for title, article_url in links_ui:
                                if len(results) >= max_results:
                                    break
                                try:
                                    art_html = await self._fetch_article_html(session, article_url)
                                    art = self._parse_article_page(art_html, article_url, fallback_title=title)
                                    if art and self._date_in_range(art.date, date_from, date_to):
                                        results.append(art)
                                except Exception:
                                    continue
                    # Без Playwright или если UI не дал ссылок — обход по категориям (только aiohttp)
                    if not results:
                        results = await self._search_via_categories(session, max_results, date_from, date_to)
                    break
                if not links:
                    break
                processed_count = 0
                successful_count = 0
                for i, (title, article_url) in enumerate(links, 1):
                    if len(results) >= max_results:
                        break
                    processed_count += 1
                    try:
                        art_html = await self._fetch_article_html(session, article_url)
                        print(f"[cyberleninka] article {i}/{len(links)} html_len={len(art_html)}", flush=True)
                        art = self._parse_article_page(art_html, article_url, fallback_title=title)
                        if art and self._date_in_range(art.date, date_from, date_to):
                            results.append(art)
                            successful_count += 1
                        elif art_html:
                            if i == 1:
                                try:
                                    (Path("debug") / "cyberleninka_article_sample.html").write_text(art_html, encoding="utf-8")
                                    print("[cyberleninka] saved first failed HTML to debug/cyberleninka_article_sample.html", flush=True)
                                except Exception:
                                    pass
                            print(f"[cyberleninka] article {i} parsed None (no title/structure)", flush=True)
                    except Exception as e:
                        print(f"[cyberleninka] article {i} error: {e}", flush=True)
                        continue
                # Продолжаем пагинацию пока:
                # 1. Не набрали max_results успешных документов И
                # 2. На странице было >= 10 ссылок (значит есть следующая страница)
                # Останавливаемся если: нет ссылок ИЛИ на странице было < 10 ссылок (конец результатов)
                if len(links) < 10:
                    print(f"[cyberleninka] page {page}: {len(links)} links (< 10), stopping pagination. Processed: {processed_count}, successful: {successful_count}, total results: {len(results)}", flush=True)
                    break
                if len(results) >= max_results:
                    print(f"[cyberleninka] reached max_results={max_results}, stopping pagination", flush=True)
                    break
                print(f"[cyberleninka] page {page}: processed {processed_count} links, {successful_count} successful, {len(results)}/{max_results} total results. Continuing to next page...", flush=True)
                page += 1
        return results[:max_results]

    async def fetch_article(self, url: str) -> SourceResult | None:
        """Загрузка одной статьи по URL."""
        if "/article/" not in url and "cyberleninka" not in url.lower():
            return None
        async with aiohttp.ClientSession() as session:
            try:
                html = await self._fetch_article_html(session, url)
                return self._parse_article_page(html, url)
            except Exception:
                return None

    def to_rag_document(
        self,
        result: SourceResult,
        *,
        chunk_size_min: int = 500,
        chunk_size_max: int = 2000,
        overlap: int = 100,
    ) -> RAGDocument:
        """Преобразование SourceResult в RAGDocument с чанкованием и валидацией."""
        import hashlib
        raw_id = f"{result.url}{result.title}"
        doc_id = hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:32]

        # При наличии PDF пробуем извлечь текст из него как основной (полнее, чем HTML)
        text_from_pdf: str | None = None
        if result.pdf_url:
            try:
                text_from_pdf = extract_text_from_pdf_url(result.pdf_url, timeout_sec=25, max_bytes=15 * 1024 * 1024)
            except Exception:
                text_from_pdf = None
        html_text = (result.full_text or "").strip()
        if text_from_pdf and len(text_from_pdf) > 500:
            text_to_chunk = text_from_pdf
        elif text_from_pdf and len(text_from_pdf) > 200 and len(text_from_pdf) >= len(html_text):
            text_to_chunk = text_from_pdf
        else:
            text_to_chunk = result.full_text or result.abstract
        # Полнота: не чанковать обрыв на середине слова — обрезать до последнего предложения
        if text_to_chunk and text_to_chunk.strip():
            _, text_to_chunk = validate_text_ends_complete(text_to_chunk)
        max_chunks_val = _text_processing_config("max_chunks_per_document", 25)
        max_chunks = int(max_chunks_val) if max_chunks_val is not None else 25
        use_emb = _text_processing_config("use_embedding_chunker", False)
        sim_thr = _text_processing_config("similarity_threshold", 0.5)
        emb_model = _text_processing_config("embedding_model", "sentence-transformers/all-MiniLM-L6-v2") or "sentence-transformers/all-MiniLM-L6-v2"
        try:
            chunks = chunk_text(
                text_to_chunk,
                chunk_size_min=chunk_size_min,
                chunk_size_max=chunk_size_max,
                overlap_tokens=overlap,
                max_chunks=max_chunks,
                use_embedding_chunker=use_emb,
                similarity_threshold=float(sim_thr) if sim_thr is not None else 0.5,
                embedding_model=emb_model,
            ) if text_to_chunk else []
        except MemoryError:
            # Очень длинный текст: один чанк из начала + аннотация
            fallback = (result.abstract or "")[:8000] if result.abstract else (text_to_chunk or "")[:8000]
            chunks = [fallback] if fallback.strip() else []
        chunks = ensure_chunks_end_at_boundaries(chunks)
        chunks = dedupe_chunks_by_hash(chunks)
        chunks = filter_and_clean_chunks(chunks)

        # Метаданные о чанках (размеры в токенах, стратегия)
        chunk_strategy = "semantic_embedding" if use_emb else "semantic_paragraph"
        chunk_info = compute_chunk_info(chunks, strategy=chunk_strategy)

        language = detect_language((result.title or "") + " " + (result.abstract or ""))

        # Метаданные: из результата + из PDF (УДК, ВАК, благодарности) + тип документа
        metadata = dict(result.metadata or {})
        if text_from_pdf:
            pdf_meta = extract_metadata_from_pdf_text(text_from_pdf)
            metadata.update(pdf_meta)
        title_abstract = (result.title or "") + " " + (result.abstract or "")
        body_preview = (text_to_chunk or "")[:1000] if text_to_chunk else ""
        metadata["document_type"] = document_type_from_text(
            result.title or "", result.abstract or "", body_preview
        )
        # Только явно диссертации/авторефераты по заголовку (не по одному упоминанию в тексте)
        title_lower = (result.title or "").lower()
        metadata["is_dissertation"] = (
            "диссертация" in title_lower or "автореферат" in title_lower
        )

        files: list[FileRef] = []
        # Ссылка на PDF и извлечённый текст для RAG (в full_text_chunks тоже используется при чанковании)
        if result.pdf_url:
            files.append(FileRef(type="PDF", url=result.pdf_url, extracted_text=text_from_pdf))

        doi_val = result.doi
        if not doi_val and (result.title or result.authors):
            try:
                doi_val = fetch_doi_from_crossref(result.title or "", result.authors, timeout_sec=3)
            except Exception:
                doi_val = result.doi
        doc = RAGDocument(
            id=doc_id,
            title=result.title or "Без названия",
            authors=result.authors,
            date=normalize_date(result.date) if result.date else result.date,
            doi=doi_val,
            url=result.url,
            language=language,
            source="CyberLeninka",
            abstract=result.abstract or "",
            full_text_chunks=chunks,
            files=files,
            metadata=metadata,
            processing_info=ProcessingInfo(
                extraction_method="rule_based",
                chunking_strategy=chunk_strategy,
                validation_score=0.0,
                chunk_info=chunk_info,
            ),
        )
        doc.processing_info.validation_score = compute_validation_score(doc)
        return doc


# Регистрация в реестре
register_source("cyberleninka", CyberLeninkaSource)
