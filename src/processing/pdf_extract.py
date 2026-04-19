"""Извлечение текста из PDF по URL (PyMuPDF, pdfplumber или pdfminer.six)."""
from __future__ import annotations

import io
import os
import sys
import urllib.request
from typing import Optional


class _SilencePdfWarnings:
    """Контекстный менеджер: подавляет stderr на уровне fd (ловит и вывод из C-библиотек, например PyMuPDF)."""

    def __enter__(self):
        self._stderr_fd = sys.stderr.fileno()
        self._devnull = open(os.devnull, "w", encoding="utf-8")
        self._saved_fd = os.dup(self._stderr_fd)
        os.dup2(self._devnull.fileno(), self._stderr_fd)
        return self

    def __exit__(self, *args):
        try:
            os.dup2(self._saved_fd, self._stderr_fd)
            os.close(self._saved_fd)
        finally:
            self._devnull.close()
        return False


def _silence_pdf_warnings():
    """Контекстный менеджер для подавления предупреждений парсера PDF."""
    return _SilencePdfWarnings()


def extract_text_from_pdf_url(
    url: str,
    timeout_sec: int = 30,
    max_bytes: int = 50 * 1024 * 1024,
) -> Optional[str]:

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; RAG-Collector/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            data = resp.read(max_bytes)
    except Exception:
        return None
    if not data or len(data) < 100:
        return None
    data_stripped = data.lstrip()
    if not data_stripped.startswith(b"%PDF"):
        return None


    try:
        import fitz  # PyMuPDF
        with _silence_pdf_warnings():
            doc = fitz.open(stream=data, filetype="pdf")
            try:
                parts = [page.get_text() for page in doc]
                text = "\n\n".join(p for p in parts if p).strip()
                return text or None
            finally:
                doc.close()
    except ImportError:
        pass
    except Exception:
        pass


    try:
        import pdfplumber
        with _silence_pdf_warnings():
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                parts = []
                for p in pdf.pages:
                    t = p.extract_text()
                    if t:
                        parts.append(t)
                text = "\n\n".join(parts).strip()
                return text or None
    except ImportError:
        pass
    except Exception:
        pass

    # 3. pdfminer.six — чистый Python, всегда ставится
    try:
        from pdfminer.high_level import extract_text_to_fp
        from pdfminer.layout import LAParams
        with _silence_pdf_warnings():
            buf = io.BytesIO(data)
            buf.seek(0)
            out = io.StringIO()
            extract_text_to_fp(buf, out, laparams=LAParams(), codec="utf-8")
            text = out.getvalue().strip()
            return text or None
    except ImportError:
        pass
    except Exception:
        pass

    return None
