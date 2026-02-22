# posms/models/registered_plus/__init__.py
"""
registered_plus model
=====================

書留 + レターパックプラス をまとめた予測モデル。

配布(exe)では MLflow 依存を避けるため、ここでは推論で必要な features のみを即時 import する。
学習系（trainer/tuner/metrics/pipeline）は遅延 import で提供する。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

# ------------------------------------------------------------
# 推論で必要なものだけ（安全：MLflow非依存）
# ------------------------------------------------------------
from posms.models.registered_plus.features import (
    TARGET,
    FEATURES_REGISTERED_PLUS,
    build_registered_plus_features,
    build_registered_plus_feature_row,  # CLI自己回帰ローリング用
)

# 型ヒント用（実行時importしない）
if TYPE_CHECKING:
    from posms.models.registered_plus.pipeline import TrainResult  # noqa: F401


# ------------------------------------------------------------
# 学習系は遅延 import（配布で mlflow を要求しない）
# ------------------------------------------------------------
def train(*args: Any, **kwargs: Any):
    from posms.models.registered_plus.trainer import train as _train
    return _train(*args, **kwargs)


def get_best_params(*args: Any, **kwargs: Any):
    from posms.models.registered_plus.tuner import get_best_params as _get_best_params
    return _get_best_params(*args, **kwargs)


def calc_metrics(*args: Any, **kwargs: Any):
    from posms.models.registered_plus.metrics import calc_metrics as _calc_metrics
    return _calc_metrics(*args, **kwargs)


def train_from_hist(*args: Any, **kwargs: Any):
    from posms.models.registered_plus.pipeline import train_from_hist as _train_from_hist
    return _train_from_hist(*args, **kwargs)


# TrainResult は型として参照されることが多いので、必要なら遅延で取得
def TrainResult():  # type: ignore[misc]
    from posms.models.registered_plus.pipeline import TrainResult as _TrainResult
    return _TrainResult


__all__ = [
    # constants / features（推論で必要）
    "TARGET",
    "FEATURES_REGISTERED_PLUS",
    "build_registered_plus_features",
    "build_registered_plus_feature_row",
    # 学習系（遅延）
    "train",
    "get_best_params",
    "calc_metrics",
    "TrainResult",
    "train_from_hist",
]