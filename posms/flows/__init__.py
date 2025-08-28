"""
posms.flows
===========

Prefect 2 で構築したワークフローレイヤー。

現在の公開フロー
----------------
monthly_refresh
    - Excel 取込 → DB ロード
    - 特徴量生成 → XGBoost 再学習 (MLflow ログ)
    - 需要予測 → PuLP でシフト最適化
    - 3 種類の Excel 出力を生成
"""

from __future__ import annotations
import logging

# ---------- ロガー ----------
logging.getLogger("posms.flows").addHandler(logging.NullHandler())

# ---------- re‑export ----------
try:
    from .monthly_flow import monthly_refresh  # noqa: F401
except ModuleNotFoundError:
    # 実装ファイルがまだ無くても import 失敗させない
    pass

__all__: list[str] = ["monthly_refresh"]
