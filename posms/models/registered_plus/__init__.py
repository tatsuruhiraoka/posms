# posms/model/registered_plus/__init__.py
"""
registered_plus model
=====================

書留 + レターパックプラス をまとめた予測モデル。

- 学習:
    train_from_hist(df_hist, ...)

- 予測（連鎖）:
    forecast_from_sum(model, df_hist, sum_future, ...)
"""

from posms.models.registered_plus.features import (
    TARGET,
    FEATURES_REGISTERED_PLUS,
    build_registered_plus_features,
    build_registered_plus_future_features,
)

from posms.models.registered_plus.trainer import train
from posms.models.registered_plus.tuner import get_best_params
from posms.models.registered_plus.predictor import predict
from posms.models.registered_plus.metrics import calc_metrics

from posms.models.registered_plus.pipeline import (
    TrainResult,
    ForecastResult,
    train_from_hist,
    forecast_from_sum,
)

__all__ = [
    # constants / features
    "TARGET",
    "FEATURES_REGISTERED_PLUS",
    "build_registered_plus_features",
    "build_registered_plus_future_features",

    # core ML functions
    "train",
    "get_best_params",
    "predict",
    "calc_metrics",

    # pipeline
    "TrainResult",
    "ForecastResult",
    "train_from_hist",
    "forecast_from_sum",
]
