"""Compatibility wrapper for :mod:`posms.etl.extract_excel`."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

__all__ = ["ExcelExtractor"]

if TYPE_CHECKING:  # pragma: no cover
    from .extract_excel import ExcelExtractor as ExcelExtractor


def __getattr__(name: str):
    if name == "ExcelExtractor":
        warnings.warn(
            "posms.etl.extractor is deprecated; use posms.etl.extract_excel instead",
            DeprecationWarning,
            stacklevel=2,
        )
        from .extract_excel import ExcelExtractor as _ExcelExtractor

        return _ExcelExtractor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + ["ExcelExtractor"])
