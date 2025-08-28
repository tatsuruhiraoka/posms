"""
Postal Operation Shift‑Management System (POSMS)
------------------------------------------------
郵便物数を予測し、その結果をもとにシフト最適化を行う
機械学習 + 数理最適化パイプライン。
"""

from __future__ import annotations

import logging
from importlib.metadata import version, PackageNotFoundError

# ----- バージョン -------------------------------------------------
try:
    __version__: str = version("posms")  # Poetry インストール後は自動解決
except PackageNotFoundError:             # 開発ツリーでの読み込み用
    __version__ = "0.0.0-dev"

# ----- パッケージ全体で使う基本ロガー ----------------------------
logging.getLogger("posms").addHandler(logging.NullHandler())

# ----- 主要 API をトップレベル re-export ------------------------
# 使いたいモジュールが実装済みになった段階でコメントアウトを外してください
try:
    from .etl.extractor import ExcelExtractor           # noqa: F401
    from .etl.loader import DbLoader                    # noqa: F401
    from .features.builder import FeatureBuilder        # noqa: F401
    from .models.trainer import ModelTrainer            # noqa: F401
    from .optimization.shift_builder import ShiftBuilder, OutputType  # noqa: F401
except ModuleNotFoundError:
    # 開発初期でまだサブモジュールが無い場合でも import エラーを抑制
    pass
