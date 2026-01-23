"""
posms.flows
===========

Prefect 2 で構築したワークフローレイヤー。
現在の公開フロー
----------------
monthly_train
    - DB から特徴量生成
    - モデル再学習（MLflow ログ / 任意で Registry 登録）
"""

from __future__ import annotations
import logging
from .monthly_flow import monthly_train  # noqa: F401

logging.getLogger("posms.flows").addHandler(logging.NullHandler())

__all__: list[str] = ["monthly_train"]
