"""Оркестратор сбора: запрос -> оптимизация -> источники -> обработка -> RAGDocument."""
import logging
import time
from typing import Optional

from .models.api import CollectRequest
from .models.document import RAGDocument
from .processing.validation import content_hash, normalize_date
from .query.optimizer import UniversalQueryOptimizer
from .sources import get_source
from .sources.base import SourceTemporarilyUnavailableError

logger = logging.getLogger(__name__)

# Cooldown после капчи/блок апров прпоа какрп карпрпировки: не обращаться к источнику N секунд (по ghjbкапк апрвоа птсо тв просто не може тос ека парва првакак сто ка кпрост как кпровп ва сторпвов рп апвроимени)
_source_unavailable_until: dict[str, float] = {}
DEFAULT_SOURCE_COOLDOWN_SEC = 300  # 5 минут


def _is_source_in_cooldown(source_name: str) -> bool:
    until = _source_unavailable_until.get(source_name, 0)
    return time.monotonic() < until


def _set_source_cooldown(source_name: str, cooldown_sec: float = DEFAULT_SOURCE_COOLDOWN_SEC) -> None:
    _source_unavailable_until[source_name] = time.monotonic() + cooldown_sec


def _get_min_abstract_length() -> int:
    """\ \ \ для \ \ «не пустым» (из coфигануно обраьб)."""
    try:
        from pathlib import Path
        import yaml
        root = Path(__file__).resolve().parent.parent
        cfg_path = root / "config" / "settings.yaml"
        if cfg_path.exists():
            with open(cfg_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            val = (data.get("validation") or {}).get("min_abstract_length")
            if val is not None:
                return max(1, int(val))
    except Exception:
        pass
    return 20


def _is_empty_document(doc: RAGDocument, min_abstract_len: Optional[int] = None) -> bool:


    if doc.full_text_chunks and len(doc.full_text_chunks) > 0:
        return False
    

    if doc.processing_info and doc.processing_info.validation_score is not None:
        if doc.processing_info.validation_score < 0.3:
            return True
    
    if min_abstract_len is None:
        min_abstract_len = _get_min_abstract_length()
    abstract = (doc.abstract or "").strip()
    

    if len(abstract) >= min_abstract_len:
        return False
    

    return True


def _get_min_keyword_matches() -> int:

    try:
        from pathlib import Path
        import yaml
        root = Path(__file__).resolve().parent.parent
        cfg_path = root / "config" / "settings.yaml"
        if cfg_path.exists():
            with open(cfg_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            val = (data.get("validation") or {}).get("min_keyword_matches")
            if val is not None:
                return max(1, int(val))

    except Exception:
        pass
    return 0


def _get_max_results_multiplier() -> int:

    try:
        from pathlib import Path
        import yaml
        root = Path(__file__).resolve().parent.parent
        cfg_path = root / "config" / "settings.yaml"
        if cfg_path.exists():
            with open(cfg_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            val = (data.get("validation") or {}).get("max_results_multiplier")
            if val is not None:
                return max(1, min(10, int(val)))
    except Exception:
        pass
    return 5


def _get_max_refetch_attempts() -> int:

    try:
        from pathlib import Path
        import yaml
        root = Path(__file__).resolve().parent.parent
        cfg_path = root / "config" / "settings.yaml"
        if cfg_path.exists():
            with open(cfg_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            val = (data.get("validation") or {}).get("max_refetch_attempts")
            if val is not None:
                return max(0, min(3, int(val)))
    except Exception:
        pass
    return 2


def _filter_by_keywords_relevance(
    documents: list[RAGDocument],
    keywords: list[str],
    min_matches: int = 2,
) -> list[RAGDocument]:

    if not keywords:
        return documents

    kw_words = []
    for kw in keywords:
        if not kw or not kw.strip():
            continue

        words = kw.strip().lower().split()
        kw_words.extend([w.strip() for w in words if len(w.strip()) >= 2])  # Минимум 2 символа
    
    if not kw_words:
        return documents
    

    seen = set()
    kw_unique = []
    for w in kw_words:
        if w not in seen:
            seen.add(w)
            kw_unique.append(w)
    

    if len(kw_unique) == 1:
        required = 1
    else:
         required = 1
    filtered = []
    for doc in documents:
        text = (doc.title or "") + " " + (doc.abstract or "") + " " + " ".join(doc.full_text_chunks or [])
        text_lower = text.lower()
        matches = sum(1 for kw in kw_unique if kw in text_lower)
        if matches >= required:
            filtered.append(doc)
        else:
            logger.debug(
                "Filtered out by keywords: title='%s...' matches=%s/%s required=%s, keywords=%s",
                (doc.title or "")[:60],
                matches,
                len(kw_unique),
                required,
                kw_unique[:3],
            )
    return filtered


async def run_collection(request: CollectRequest) -> list[RAGDocument]:

    optimizer = UniversalQueryOptimizer()
    date_range = request.date_range()
    seen_hashes: set[str] = set()
    all_docs: list[RAGDocument] = []
    logger.info(
        "Starting collection: sources=%s, max_results=%s per source, keywords=%s",
        request.sources,
        request.max_results,
        request.keywords,
    )


    if request.task_type == "target_site" and request.urls:
        processed_urls: set[str] = set()  # Для отслеживания уже обработанных URL
        for source_name in request.sources:
            source = get_source(source_name)
            if not source or not hasattr(source, "fetch_article"):
                continue
            if _is_source_in_cooldown(source_name):
                logger.warning("Source %s skipped (cooldown) for target_site.", source_name)
                continue
            try:
                for url in request.urls:
                    url = url.strip()
                    if not url or url in processed_urls:
                        continue
                    try:
                        res = await source.fetch_article(url)
                        if res and hasattr(source, "to_rag_document"):
                            doc = source.to_rag_document(res)
                            if _is_empty_document(doc):
                                continue
                            h = content_hash(doc)
                            if h not in seen_hashes:
                                seen_hashes.add(h)
                                all_docs.append(doc)
                                processed_urls.add(url)
                    except SourceTemporarilyUnavailableError as e:
                        _set_source_cooldown(e.source_name or source_name, DEFAULT_SOURCE_COOLDOWN_SEC)
                        logger.warning("Source %s temporarily unavailable: %s. Cooldown set.", source_name, e)
                        break
                    except Exception:
                        continue
            finally:
                if hasattr(source, "close"):
                    try:
                        await source.close()
                    except Exception:
                        pass

        remaining_urls = [u.strip() for u in request.urls if u.strip() and u.strip() not in processed_urls]
        if remaining_urls:
            universal_source = get_source("universal_url")
            if universal_source and hasattr(universal_source, "fetch_article"):
                logger.info("[target_site] Fallback: parsing %d remaining URLs with universal_url", len(remaining_urls))
                print(f"[collector] Fallback: parsing {len(remaining_urls)} URLs with universal_url", flush=True)
                try:
                    for url in remaining_urls:
                        try:
                            res = await universal_source.fetch_article(url)
                            if res and hasattr(universal_source, "to_rag_document"):
                                doc = universal_source.to_rag_document(res)
                                if _is_empty_document(doc):
                                    continue
                                h = content_hash(doc)
                                if h not in seen_hashes:
                                    seen_hashes.add(h)
                                    all_docs.append(doc)
                        except Exception as e:
                            logger.warning("[universal_url] Failed to parse %s: %s", url[:100], e)
                            continue
                finally:
                    if hasattr(universal_source, "close"):
                        try:
                            await universal_source.close()
                        except Exception:
                            pass

        return all_docs


    logger.info("Starting keyword search mode with sources: %s", request.sources)
    print(f"[collector] Starting keyword search mode with sources: {request.sources}", flush=True)
    for source_name in request.sources:
        logger.info("[%s] processing source...", source_name)
        print(f"[collector] Processing source: {source_name}", flush=True)
        try:
            source = get_source(source_name)
            if not source:
                logger.warning("[%s] source not found or not available, skipping", source_name)
                print(f"[collector] WARNING: {source_name} source not found or not available, skipping", flush=True)
                continue
            if _is_source_in_cooldown(source_name):
                logger.warning(
                    "[%s] skipped: in cooldown (CAPTCHA/blocked). Will retry after cooldown.",
                    source_name,
                )
                print(f"[collector] WARNING: {source_name} skipped: in cooldown (CAPTCHA/blocked)", flush=True)
                continue
            try:
                query = optimizer.optimize_query(
                    source=source_name,
                    keywords=request.keywords,
                    date_range=date_range,
                    languages=request.languages,
                )
            except Exception as e:
                logger.error("[%s] optimize_query() failed: %s", source_name, e, exc_info=True)
                query = None
            if not query:
                query = " ".join(k.strip() for k in request.keywords if k)
                logger.info("[%s] using fallback query: '%s'", source_name, query[:100])
        except Exception as e:
            logger.exception("[%s] failed during initialization: %s. Skipping source.", source_name, e)
            print(f"[collector] ERROR: {source_name} failed during initialization: {e}", flush=True)
            import traceback
            print(f"[collector] Traceback for {source_name}: {traceback.format_exc()}", flush=True)
            continue

        try:
            if not hasattr(source, "search"):
                logger.warning("[%s] source does not have search() method, skipping", source_name)
                print(f"[collector] WARNING: {source_name} does not have search() method, skipping", flush=True)
                continue
            
            logger.info(
                "[%s] starting search: query='%s', max_results=%s, date_range=%s..%s, languages=%s",
                source_name,
                query[:100] if query else "",
                request.max_results,
                request.date_from or "*",
                request.date_to or "*",
                request.languages or [],
            )
            print(f"[collector] {source_name} starting search: query='{query[:100] if query else ''}', max_results={request.max_results}", flush=True)
            added_from_source = 0
            skipped_empty = 0
            skipped_dup = 0
            max_refetch_attempts = _get_max_refetch_attempts()
            # Для медленных источников (Playwright) уменьшаем количество повторных попыток
            if source_name == "cyberleninka":
                max_refetch_attempts = min(max_refetch_attempts, 1)  # Максимум 1 повторная попытка для CyberLeninka
            refetch_attempt = 0
            total_results_fetched = 0
            
            # Цикл повторных запросов для добра документов до нужного количества
            while added_from_source < request.max_results and refetch_attempt <= max_refetch_attempts:
                # Для всех источников используем множитель для компенсации отбраковки пустых/невалидных
                multiplier = _get_max_results_multiplier()
                # Базовый множитель + дополнительный при повторных запросах
                fetch_limit = request.max_results * (multiplier + refetch_attempt * 2)
                
                # Ограничиваем максимальный лимит запроса (чтобы не перегружать источники)
                max_fetch_limit = request.max_results * 15
                fetch_limit = min(fetch_limit, max_fetch_limit)
                
                if refetch_attempt > 0:
                    logger.info(
                        "[%s] refetch attempt %s/%s: requesting %s results to reach %s docs (currently have %s)",
                        source_name,
                        refetch_attempt,
                        max_refetch_attempts,
                        fetch_limit,
                        request.max_results,
                        added_from_source,
                    )
                
                try:
                    results = await source.search(
                        query,
                        max_results=fetch_limit,
                        date_from=request.date_from,
                        date_to=request.date_to,
                        languages=request.languages,
                    )
                except Exception as search_error:
                    logger.error(
                        "[%s] search() failed in refetch attempt %s: %s",
                        source_name,
                        refetch_attempt,
                        search_error,
                        exc_info=True,
                    )
                    break  # Прекращаем повторные запросы при ошибке
                
                total_results_fetched += len(results)
                
                if refetch_attempt == 0:
                    logger.info(
                        "[%s] fetched %s raw results (requested %s)",
                        source_name,
                        len(results),
                        fetch_limit,
                    )
                    if len(results) == 0:
                        logger.warning(
                            "[%s] returned 0 results for query='%s'. Check query format or source availability.",
                            source_name,
                            query[:100] if query else "",
                        )
                
                # Обрабатываем результаты
                processed_in_this_batch = 0
                for res in results:
                    if added_from_source >= request.max_results:
                        break
                    if hasattr(source, "to_rag_document"):
                        # Логируем SourceResult перед преобразованием
                        print(f"[collector] {source_name} converting SourceResult: title='{(res.title or '')[:60]}', abstract_len={len(res.abstract or '')}, url={res.url[:80] if res.url else 'N/A'}", flush=True)
                        doc = source.to_rag_document(res)
                        # Логируем RAGDocument после преобразования
                        print(f"[collector] {source_name} converted to RAGDocument: title='{(doc.title or '')[:60]}', abstract_len={len(doc.abstract or '')}, chunks={len(doc.full_text_chunks or [])}, validation_score={doc.processing_info.validation_score if doc.processing_info else None}", flush=True)
                    else:
                        continue
                    if _is_empty_document(doc):
                        skipped_empty += 1
                        logger.info(
                            "[%s] skipped empty doc: title='%s' (len=%s), abstract_len=%s, chunks=%s, validation_score=%s",
                            source_name,
                            (doc.title or "")[:50],
                            len(doc.title or ""),
                            len(doc.abstract or ""),
                            len(doc.full_text_chunks or []),
                            doc.processing_info.validation_score if doc.processing_info else None,
                        )
                        continue
                    # Фильтруем документы с очень низким validation_score (< 0.3)
                    if doc.processing_info and doc.processing_info.validation_score is not None:
                        if doc.processing_info.validation_score < 0.3:
                            skipped_empty += 1
                            logger.info(
                                "[%s] skipped low validation_score doc: title='%s' score=%s",
                                source_name,
                                (doc.title or "")[:50],
                                doc.processing_info.validation_score,
                            )
                            continue
                    h = content_hash(doc)
                    if h in seen_hashes:
                        skipped_dup += 1
                        logger.info("[%s] skipped duplicate doc: title='%s'", source_name, (doc.title or "")[:50])
                        continue
                    seen_hashes.add(h)
                    all_docs.append(doc)
                    added_from_source += 1
                    processed_in_this_batch += 1
                
                # Если в этой попытке не добавили новых документов, прекращаем повторные запросы
                if processed_in_this_batch == 0:
                    logger.info(
                        "[%s] no new documents in refetch attempt %s, stopping refetch to avoid delays",
                        source_name,
                        refetch_attempt,
                    )
                    break
                
                # Если достигли нужного количества документов, прекращаем
                if added_from_source >= request.max_results:
                    break
                
                # Если это была последняя попытка или нет результатов для обработки, прекращаем
                if refetch_attempt >= max_refetch_attempts or len(results) == 0:
                    break
                
                refetch_attempt += 1
            
            # Логирование ПОСЛЕ цикла
            if added_from_source == 0 and total_results_fetched > 0:
                logger.warning(
                    "[%s] added 0 docs (results=%s, skipped_empty=%s, skipped_dup=%s, refetch_attempts=%s). "
                    "All documents were filtered as empty or duplicates.",
                    source_name,
                    total_results_fetched,
                    skipped_empty,
                    skipped_dup,
                    refetch_attempt,
                )
                print(f"[collector] {source_name}: added 0 docs (total_fetched={total_results_fetched}, skipped_empty={skipped_empty}, skipped_dup={skipped_dup})", flush=True)
            elif added_from_source:
                logger.info(
                    "[%s] added %s/%s docs (refetch_attempts=%s, total_fetched=%s, skipped_empty=%s, skipped_dup=%s)",
                    source_name,
                    added_from_source,
                    request.max_results,
                    refetch_attempt,
                    total_results_fetched,
                    skipped_empty,
                    skipped_dup,
                )
                print(f"[collector] {source_name}: added {added_from_source}/{request.max_results} docs (total_fetched={total_results_fetched}, skipped_empty={skipped_empty}, skipped_dup={skipped_dup})", flush=True)
            else:
                # Логируем даже если 0 результатов - это важно для диагностики
                if total_results_fetched == 0:
                    logger.warning(
                        "[%s] returned 0 raw results from source.search() - check query format or source availability",
                        source_name,
                    )
                else:
                    logger.warning(
                        "[%s] added 0 docs (total_fetched=%s, skipped_empty=%s, skipped_dup=%s, refetch_attempts=%s). "
                        "All documents were filtered out.",
                        source_name,
                        total_results_fetched,
                        skipped_empty,
                        skipped_dup,
                        refetch_attempt,
                    )
        except SourceTemporarilyUnavailableError as e:
            _set_source_cooldown(e.source_name or source_name, DEFAULT_SOURCE_COOLDOWN_SEC)
            logger.warning(
                "[%s] temporarily unavailable: %s. Skipping; cooldown %s sec. Other sources continue.",
                e.source_name or source_name,
                e,
                DEFAULT_SOURCE_COOLDOWN_SEC,
            )
            continue
        except Exception as e:
            logger.exception("[%s] failed with exception: %s. Skipping; other sources continue.", source_name, e)
            print(f"[collector] ERROR: {source_name} failed with exception: {e}", flush=True)
            import traceback
            print(f"[collector] Traceback for {source_name}: {traceback.format_exc()}", flush=True)
            continue
        finally:
            # Закрываем сессию источника, если есть метод close()
            if hasattr(source, "close"):
                try:
                    await source.close()
                except Exception:
                    pass  # Игнорируем ошибки при закрытии

    # Финальная фильтрация по дате: оставляем только документы в [date_from, date_to].
    # Дублирует/подстраховывает фильтры источников (OpenAlex, PubMed, arXiv, CiNii, CrossRef);
    # документы без даты или с непарсящейся датой при активном фильтре отбрасываются.
    if request.date_from or request.date_to:
        filtered = []
        dropped_no_date = 0
        dropped_out_of_range = 0
        dropped_docs_info = []
        for doc in all_docs:
            norm = normalize_date(doc.date) if doc.date else None
            if not norm:
                dropped_no_date += 1
                dropped_docs_info.append(f"no_date: {doc.title[:50] if doc.title else 'no title'} (date={doc.date})")
                continue
            if request.date_from and norm < request.date_from:
                dropped_out_of_range += 1
                dropped_docs_info.append(f"too_old: {doc.title[:50] if doc.title else 'no title'} (date={norm}, min={request.date_from})")
                continue
            if request.date_to and norm > request.date_to:
                dropped_out_of_range += 1
                dropped_docs_info.append(f"too_new: {doc.title[:50] if doc.title else 'no title'} (date={norm}, max={request.date_to})")
                continue
            filtered.append(doc)
        if dropped_no_date or dropped_out_of_range:
            logger.info(
                "Date filter: %s -> %s (dropped no_date=%s, out_of_range=%s, range=%s..%s)",
                len(all_docs),
                len(filtered),
                dropped_no_date,
                dropped_out_of_range,
                request.date_from or "*",
                request.date_to or "*",
            )
            print(f"[collector] Date filter: {len(all_docs)} -> {len(filtered)} (dropped no_date={dropped_no_date}, out_of_range={dropped_out_of_range})", flush=True)
            if dropped_docs_info:
                logger.info("Dropped documents details (first 10): %s", dropped_docs_info[:10])
                print(f"[collector] Dropped documents (first 5): {dropped_docs_info[:5]}", flush=True)
        all_docs = filtered

    # Пост-фильтрация по релевантности: оставляем документы, где в title, abstract или full_text_chunks
    # встречается не менее min_keyword_matches ключевых слов (для улучшения релевантности результатов)
    before_kw_filter = len(all_docs)
    if request.keywords:
        all_docs = _filter_by_keywords_relevance(
            all_docs,
            request.keywords,
            min_matches=_get_min_keyword_matches(),
        )
        if before_kw_filter > len(all_docs):
            filtered_out_count = before_kw_filter - len(all_docs)
            logger.info(
                "Post-filter by keywords: %s -> %s documents (min_matches=%s, keywords=%s, filtered_out=%s)",
                before_kw_filter,
                len(all_docs),
                _get_min_keyword_matches(),
                request.keywords,
                filtered_out_count,
            )
            print(f"[collector] Keyword filter: {before_kw_filter} -> {len(all_docs)} documents (filtered_out={filtered_out_count}, min_matches={_get_min_keyword_matches()}, keywords={request.keywords})", flush=True)
        elif before_kw_filter == 0:
            logger.warning(
                "No documents before keyword filter - all sources returned 0 or were filtered earlier"
            )
            print(f"[collector] WARNING: No documents before keyword filter", flush=True)

    logger.info(
        "Collection completed: %s documents total (from %s sources, max_results=%s per source)",
        len(all_docs),
        len(request.sources),
        request.max_results,
    )
    print(f"[collector] Collection completed: {len(all_docs)} documents total (from {len(request.sources)} sources)", flush=True)
    return all_docs
