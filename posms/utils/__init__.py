"""
posms.utils
===========

POSMS 全体で再利用する小物ユーティリティを格納します。

* **db**      : SQLAlchemy の SessionManager／接続ヘルパー
* **logger**  : プロジェクト標準ロガー設定 (`setup_logger`)
"""

from __future__ import annotations
import logging

# ------- Base logger -------
logging.getLogger("posms.utils").addHandler(logging.NullHandler())

# ------- Public re‑exports -------
try:
    from .db import SessionManager  # noqa: F401
except ModuleNotFoundError:  # 開発初期で未実装でも import 失敗させない
    pass

try:
    from .logger import setup_logger  # noqa: F401
except ModuleNotFoundError:
    pass

__all__: list[str] = ["SessionManager", "setup_logger"]
