# posms/utils/logger.py
"""
posms.utils.logger
==================

アプリ／スクリプト側で使う **ロガー初期化ユーティリティ**。

概要
----
- **ゼロ設定**：環境変数 `LOG_LEVEL`（未指定なら `INFO`）でログレベルを決定
- **二重初期化防止**：`setup_logger()` は **idempotent**（重ねて呼んでも安全）
- **コンソール + 任意でファイル** に出力（UTF-8）
- ライブラリ側の方針に合わせ、**import 時には何もしません**
  （必要なときだけ `setup_logger()` を呼ぶ）

使い方
------
>>> from posms.utils.logger import setup_logger, get_logger
>>> setup_logger(log_level="DEBUG", to_file=True, log_dir="logs")
>>> log = get_logger(__name__)
>>> log.info("hello")

環境変数
--------
- `LOG_LEVEL`（例: `DEBUG` / `INFO` / `WARNING` ...）
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

__all__ = ["setup_logger", "get_logger"]

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LEVEL = "INFO"


def _resolve_level(level_name: str) -> int:
    """文字列レベル → logging レベルへ解決（不正値は INFO にフォールバック）"""
    return getattr(logging, (level_name or "").upper(), logging.INFO)


def _has_handler(root: logging.Logger, tag: str) -> bool:
    """当ユーティリティが付与したハンドラ（tag属性）を検出"""
    return any(getattr(h, "_posms_tag", None) == tag for h in root.handlers)


def setup_logger(
    log_level: Optional[str] = None,
    to_file: bool = False,
    log_dir: str | Path = "logs",
) -> None:
    """
    ルートロガーを設定（**idempotent**）。

    Parameters
    ----------
    log_level : str | None
        "DEBUG" / "INFO" / "WARNING" / "ERROR" / "CRITICAL"。未指定なら環境変数 LOG_LEVEL→INFO。
    to_file : bool
        True なら ``log_dir/%Y-%m-%d_%H%M%S_APP.log`` にも出力を追加。
    log_dir : str | Path
        ファイル出力先ディレクトリ（存在しなければ作成）。
    """
    root = logging.getLogger()
    root.setLevel(_resolve_level(log_level or os.getenv("LOG_LEVEL", DEFAULT_LEVEL)))

    # --- コンソール出力（重複追加を防止） ---
    if not _has_handler(root, "console"):
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
        ch._posms_tag = "console"  # type: ignore[attr-defined]
        root.addHandler(ch)

    # --- ファイル出力（要求があり、まだ未追加なら） ---
    if to_file and not _has_handler(root, "file"):
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        fname = datetime.now().strftime("%Y-%m-%d_%H%M%S_APP.log")
        fh = logging.FileHandler(log_path / fname, encoding="utf-8")
        fh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
        fh._posms_tag = "file"  # type: ignore[attr-defined]
        root.addHandler(fh)


def get_logger(name: str) -> logging.Logger:
    """
    モジュール用ロガーを取得。まだ未設定ならデフォルトで `setup_logger()` を適用。
    """
    # 既に当ユーティリティのコンソールハンドラがあれば初期化済みとみなす
    if not _has_handler(logging.getLogger(), "console"):
        setup_logger()
    return logging.getLogger(name)
