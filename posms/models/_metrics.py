"""_metrics.py
=================
RMSE（Root Mean Squared Error）を 1 行で安全に計算する小さなユーティリティです
scikit-learn のバージョン差による API 違いを自動で吸収します。
新しめの scikit-learn にある root_mean_squared_error が使えるならそれを使用。
ない環境では mean_squared_error(..., squared=False) にフォールバックします。
multioutput="raw_values" などで 配列が返るケースでも float へ正規化（平均に丸める）し、常に float を返します。
"""

from __future__ import annotations

from typing import Any
import numpy as np
from sklearn.metrics import mean_squared_error

try:  # scikit-learn >= 1.3 で提供
    from sklearn.metrics import root_mean_squared_error as _rmse  # type: ignore
except ImportError:  # 依存が古い場合のみフォールバック
    _rmse = None  # type: ignore

__all__ = ["rmse"]


def _to_float(value: Any) -> float:
    """スカラーはそのまま float、配列（raw_values 等）は平均に丸めて float を返す。"""
    try:
        return float(value)
    except (TypeError, ValueError):
        arr = np.asarray(value, dtype=float)
        return float(arr.mean())


def rmse(y_true: Any, y_pred: Any, **kw: Any) -> float:
    """Root Mean Squared Error を返すユーティリティ。

    scikit-learn >= 1.3 では `root_mean_squared_error` を使用。
    それ未満では `mean_squared_error(..., squared=False)` へフォールバック。
    `multioutput="raw_values"` のように配列が返る場合は平均して float を返します。
    """
    if _rmse is not None:  # pragma: no branch
        return _to_float(_rmse(y_true, y_pred, **kw))
    # 古い sklearn では mean_squared_error に squared 引数が無い
    try:
        return _to_float(mean_squared_error(y_true, y_pred, squared=False, **kw))
    except TypeError:
        # 'squared' を受け付けない → 通常の MSE を √ して RMSE 化
        kw.pop("squared", None)
        return _to_float(np.sqrt(mean_squared_error(y_true, y_pred, **kw)))
