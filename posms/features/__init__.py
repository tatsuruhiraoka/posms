"""
posms.features
==============

特徴量エンジニアリング層。

主な公開クラス
--------------
FeatureBuilder
    - 郵便物 raw テーブルを読み込み
    - 曜日 / 祝日 / 過去ラグ / 移動平均 などの特徴量を生成
    - モデル学習・推論に使う DataFrame / ndarray を返す
"""

from __future__ import annotations

import logging

# ---------------- Logger -----------------
logging.getLogger("posms.features").addHandler(logging.NullHandler())

# --------------- re‑export ---------------
try:
    from .builder import FeatureBuilder  # noqa: F401
except ModuleNotFoundError:
    # 開発初期で未実装でも import エラーにしない
    pass

__all__: list[str] = ["FeatureBuilder"]
