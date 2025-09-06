"""
Postal Operation Shift-Management System (POSMS)
------------------------------------------------
郵便物数を予測し、その結果をもとにシフト最適化を行う
機械学習 + 数理最適化パイプライン。

トップレベルAPI（遅延 re-export）
--------------------------------
- ExcelExtractor        … posms.etl.extract_excel.ExcelExtractor
- DbLoader              … posms.etl.load_to_db.DbLoader
- FeatureBuilder        … posms.features.builder.FeatureBuilder
- ModelTrainer          … posms.models.trainer.ModelTrainer
- ShiftBuilder, OutputType … posms.optimization.shift_builder
"""

from __future__ import annotations

import importlib
import logging
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING

# ----- バージョン -------------------------------------------------
try:
    __version__: str = version("posms")  # インストール後はメタデータから取得
except PackageNotFoundError:  # 開発ツリーでの読み込み用
    __version__ = "0.0.0-dev"

# ----- パッケージ全体で使う基本ロガー ----------------------------
logging.getLogger("posms").addHandler(logging.NullHandler())

# ----- 遅延 re-export 用の公開名 --------------------------------
__all__ = [
    "ExcelExtractor",
    "DbLoader",
    "FeatureBuilder",
    "ModelTrainer",
    "ShiftBuilder",
    "OutputType",
    "__version__",
]

# 型チェッカー/IDE向け（実行時は読み込まれない）
if TYPE_CHECKING:  # pragma: no cover
    from .etl.extract_excel import ExcelExtractor as ExcelExtractor  # noqa: F401
    from .etl.load_to_db import DbLoader as DbLoader  # noqa: F401
    from .features.builder import FeatureBuilder as FeatureBuilder  # noqa: F401
    from .models.trainer import ModelTrainer as ModelTrainer  # noqa: F401
    from .optimization.shift_builder import (  # noqa: F401
        ShiftBuilder as ShiftBuilder,
        OutputType as OutputType,
    )


def __getattr__(name: str):
    """
    遅延でサブモジュールから公開クラスを re-export する。
    import 時の副作用（重い依存の読み込み・Deprecated 警告）を避けるため。
    """
    try:
        if name == "ExcelExtractor":
            # ※ deprecated な posms.etl.extractor ではなく、新パスから提供
            mod = importlib.import_module("posms.etl.extract_excel")
            val = getattr(mod, "ExcelExtractor")
        elif name == "DbLoader":
            mod = importlib.import_module("posms.etl.load_to_db")
            val = getattr(mod, "DbLoader")
        elif name == "FeatureBuilder":
            mod = importlib.import_module("posms.features.builder")
            val = getattr(mod, "FeatureBuilder")
        elif name == "ModelTrainer":
            mod = importlib.import_module("posms.models.trainer")
            val = getattr(mod, "ModelTrainer")
        elif name in {"ShiftBuilder", "OutputType"}:
            mod = importlib.import_module("posms.optimization.shift_builder")
            val = getattr(mod, name)
        else:
            raise AttributeError(name)
    except ModuleNotFoundError as exc:  # 依存不足や未実装を親切に通知
        raise ImportError(
            f"{name} を利用するには依存関係が不足しています: {exc}"
        ) from exc

    # キャッシュして次回以降の属性解決を高速化
    globals()[name] = val
    return val


def __dir__() -> list[str]:  # pragma: no cover
    return sorted(__all__)
