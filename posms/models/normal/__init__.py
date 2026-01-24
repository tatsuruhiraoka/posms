# posms/models/__init__.py
"""posms.models
=================

機械学習モデル層（学習・推論ヘルパー）。

概要
----
* **ModelTrainer**   … 特徴量 DataFrame を受け取り、XGBoost で学習し MLflow に記録（ゼロ設定対応）
* **ModelPredictor** … MLflow から指定 run_id / ステージのモデルをロードして推論

使い方
------
学習:
    >>> from posms.models import ModelTrainer
    >>> from posms.features import FeatureBuilder
    >>> X, y = FeatureBuilder().build()
    >>> run_id = ModelTrainer().train(X, y, auto_register=True)

推論:
    >>> from posms.models import ModelPredictor
    >>> pred = ModelPredictor(stage="Production").predict(X.iloc[:1])[0]

ゼロ設定 MLflow
--------------
MLFLOW_TRACKING_URI を未設定の場合、リポジトリ直下 ``mlruns/`` を自動利用します。
Model Registry が利用できない環境では、Predictor は最新の run を自動選択して推論します。
"""

from __future__ import annotations

import importlib
import logging
from importlib import metadata
from typing import TYPE_CHECKING

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# ここで __version__ を確定（Ruff F822 回避 & IDE 補完向上）
try:
    __version__: str = metadata.version("posms")
except Exception:  # pragma: no cover
    __version__ = "0+unknown"

__all__ = ["ModelTrainer", "ModelPredictor", "__version__"]


def __getattr__(name: str):
    """遅延でサブモジュールから公開クラスを re-export する。"""
    if name == "ModelTrainer":
        module = importlib.import_module("posms.models.trainer")
        value = getattr(module, "ModelTrainer")
    elif name == "ModelPredictor":
        module = importlib.import_module("posms.models.predictor")
        value = getattr(module, "ModelPredictor")
    else:
        raise AttributeError(name)

    # キャッシュして次回以降の解決を高速化
    globals()[name] = value
    return value


def __dir__() -> list[str]:  # pragma: no cover
    return sorted(__all__)


# 型チェッカー/IDE 補完向け（実行時は読み込まれない）
if TYPE_CHECKING:  # pragma: no cover
    from .trainer import ModelTrainer  # noqa: F401
    from .predictor import ModelPredictor  # noqa: F401

    try:
        from .tuner import tune_xgb_optuna
    except Exception:
        tune_xgb_optuna = None  # type: ignore

    __all__ = ["ModelTrainer", "ModelPredictor", "tune_xgb_optuna"]
