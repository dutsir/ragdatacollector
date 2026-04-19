"""Data source adapters (CyberLeninka, Google SERP, etc.)."""
from .base import BaseSource, SourceResult
from .registry import get_source, list_sources, register_source

# Регистрация источников
from . import cyberleninka  # noqa: F401
from . import crossref  # noqa: F401
from . import openalex  # noqa: F401
from . import pubmed  # noqa: F401
from . import arxiv  # noqa: F401
from . import cinii  # noqa: F401
from . import duckduckgo_serp  # noqa: F401
from . import universal_url  # noqa: F401

__all__ = ["BaseSource", "SourceResult", "get_source", "list_sources", "register_source"]
