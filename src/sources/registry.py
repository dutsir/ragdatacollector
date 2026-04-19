"""Реестр источников для расширяемости."""
from __future__ import annotations

from typing import Type

from .base import BaseSource

_SOURCES: dict[str, Type[BaseSource]] = {}


def register_source(name: str, source_class: Type[BaseSource]) -> None:
    _SOURCES[name] = source_class


def get_source(name: str) -> BaseSource | None:
    cls = _SOURCES.get(name)
    return cls() if cls else None


def list_sources() -> list[str]:
    return list(_SOURCES.keys())
