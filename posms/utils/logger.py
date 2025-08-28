"""
posms.utils.logger
==================

プロジェクト全体で統一したログ設定を行うユーティリティ。

* ``setup_logger()`` を最初に 1 回呼び出すだけで、
  - LOG_LEVEL／ENVIRONMENT を .env または環境変数から取得
  - コンソールと (任意) ファイルに同時出力
* ``get_logger(__name__)`` で任意モジュールからロガー取得
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
DEFAULT_LEVEL = "INFO"


def _resolve_level(level_name: str) -> int:
    return getattr(logging, level_name.upper(), logging.INFO)


def setup_logger(
    log_level: Optional[str] = None,
    to_file: bool = False,
    log_dir: str | Path = "logs",
) -> None:
    """
    グローバルロガー設定を行う（ idempotent ）

    Parameters
    ----------
    log_level : str | None
        DEBUG / INFO / WARNING / ERROR / CRITICAL
    to_file : bool
        True の場合、logs/yyyy-mm-dd_APP.log にも出力
    log_dir : str | Path
        ファイル出力先ディレクトリ
    """
    if getattr(setup_logger, "_configured", False):
        return  # 二重設定を防止

    lvl = log_level or os.getenv("LOG_LEVEL", DEFAULT_LEVEL)
    logging.basicConfig(level=_resolve_level(lvl), format=LOG_FORMAT)

    if to_file:
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True, parents=True)
        fname = datetime.now().strftime("%Y-%m-%d_%H%M%S_APP.log")
        fh = logging.FileHandler(log_path / fname, encoding="utf-8")
        fh.setFormatter(logging.Formatter(LOG_FORMAT))
        root = logging.getLogger()
        root.addHandler(fh)

    setup_logger._configured = True  # type: ignore[attr-defined]


def get_logger(name: str) -> logging.Logger:
    """モジュールから呼び出す簡易ヘルパー"""
    if not getattr(setup_logger, "_configured", False):
        setup_logger()  # デフォルト設定で初期化
    return logging.getLogger(name)


# -------------- 自動初期化 --------------
# CLI やモジュールで import しただけでも INFO レベルで動く
setup_logger()
