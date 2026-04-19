"""Определение языка контента (langdetect, с fallback)."""
from typing import Optional

# Поддержка RTL и CJK через стандартные коды
SUPPORTED_LANG_CODES = {
    "ru", "en", "fr", "de", "es", "zh-cn", "zh", "ja", "ko", "ar", "he", "yi",
}
# Маппинг langdetect -> наш код
LANG_NORMALIZE = {
    "zh-cn": "zh",
    "zh-tw": "zh",
}


def detect_language(text: str) -> str:
    """
    Определяет язык текста. Возвращает код языка (ru, en, zh, ...).
    При ошибке или пустом тексте возвращает 'en'.
    """
    if not text or not text.strip():
        return "en"
    text = text.strip()
    if len(text) < 20:
        return "en"
    try:
        from langdetect import detect, DetectorFactory
        DetectorFactory.seed = 0
        lang = detect(text)
        lang = lang.lower()
        lang = LANG_NORMALIZE.get(lang, lang)
        if lang in SUPPORTED_LANG_CODES or lang.split("-")[0] in SUPPORTED_LANG_CODES:
            return lang.split("-")[0]
        return lang
    except Exception:
        return "en"


def is_rtl(lang: str) -> bool:
    """Проверка RTL языка (арабский, иврит)."""
    return lang in ("ar", "he", "yi")


def is_cjk(lang: str) -> bool:
    """Проверка CJK языка."""
    return lang in ("zh", "ja", "ko")
