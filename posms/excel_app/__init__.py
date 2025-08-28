"""
posms.excel_app
===============

Excel から POSMS の各処理をボタン一つで実行できる
**Xlwings アドイン** を提供するサブパッケージ。

主な機能
--------
* 需要予測・シフト最適化のトリガー
* 結果のワークブック自動更新
* ログ表示パネル

Example
-------
>>> from posms.excel_app import XlwingsRunner
>>> XlwingsRunner().start()
"""

from __future__ import annotations
import logging

logging.getLogger("posms.excel_app").addHandler(logging.NullHandler())

try:
    from .runner import XlwingsRunner  # noqa: F401
except ModuleNotFoundError:
    # runner.py が未実装の段階でもパッケージ import を失敗させない
    pass

__all__: list[str] = ["XlwingsRunner"]
