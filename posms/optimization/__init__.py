# posms/optimization/__init__.py
from __future__ import annotations

"""
posms.optimization

最適化（シフト作成）関連の公開API。
__init__ では重い依存（pulp/openpyxl）を即 import しないために lazy import を採用する。

公開API（予定）:
- GridLayout
- apply_assignments_to_grid_xlsm
- ShiftBuilderGrid
- run  （runner入口: params(dict) -> dict を想定）
"""

from typing import TYPE_CHECKING, Any
import importlib

__all__ = [
    "GridLayout",
    "apply_assignments_to_grid_xlsm",
    "ShiftBuilderGrid",
    "run",
]

if TYPE_CHECKING:
    # 型チェック用（実行時には import されない）
    from .shift_builder_grid import GridLayout, apply_assignments_to_grid_xlsm
    from .shift_builder_grid_solver import ShiftBuilderGrid
    from .shift_builder_grid_runner import run


def __getattr__(name: str) -> Any:
    """
    遅延 import:
      from posms.optimization import run
    のような使い方を可能にしつつ、posms.optimization import 時の副作用を避ける。
    """
    if name in ("GridLayout", "apply_assignments_to_grid_xlsm"):
        mod = importlib.import_module(".shift_builder_grid", __name__)
        return getattr(mod, name)

    if name == "ShiftBuilderGrid":
        mod = importlib.import_module(".shift_builder_grid_solver", __name__)
        return getattr(mod, name)

    if name == "run":
        mod = importlib.import_module(".shift_builder_grid_runner", __name__)
        return getattr(mod, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
