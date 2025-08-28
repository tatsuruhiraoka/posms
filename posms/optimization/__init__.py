"""
posms.optimization
==================

シフト最適化サブパッケージ。

* **ShiftBuilder** ―― 需要予測データと社員マスタを入力に、
  PuLP で最適化して 3 種類の Excel 出力（分担表 / 勤務指定表 / 分担表案）
  を生成します。
"""

from __future__ import annotations
import logging

logging.getLogger("posms.optimization").addHandler(logging.NullHandler())

# トップレベル re‑export
try:
    from .shift_builder import ShiftBuilder, OutputType  # noqa: F401
except ModuleNotFoundError:
    # 実装ファイルがまだ無くても import 失敗させない
    pass

__all__: list[str] = ["ShiftBuilder", "OutputType"]
