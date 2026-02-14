# posms/model/registered_plus/__init__.py
"""
registered_plus model
=====================

書留 + レターパックプラス をまとめた予測モデル。

- 学習:
    train_from_hist(df_hist, ...)

運用での予測（forecast）は CLI 側で
ModelPredictor(model_name="posms_registered_plus", ...) を使って実行する。
"""

from posms.models.registered_plus.features import (
    TARGET,
    FEATURES_REGISTERED_PLUS,
    build_registered_plus_features,
    build_registered_plus_feature_row,      # CLI自己回帰ローリング用
)

from posms.models.registered_plus.trainer import train
from posms.models.registered_plus.tuner import get_best_params
from posms.models.registered_plus.metrics import calc_metrics

from posms.models.registered_plus.pipeline import (
    TrainResult,
    train_from_hist,
)

__all__ = [
    # constants / features
    "TARGET",
    "FEATURES_REGISTERED_PLUS",
    "build_registered_plus_features",
    "build_registered_plus_future_features",
    "build_registered_plus_feature_row",
    # core ML functions
    "train",
    "get_best_params",
    "calc_metrics",
    # pipeline
    "TrainResult",
    "train_from_hist",
]
