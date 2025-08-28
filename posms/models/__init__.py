"""
posms.models
============

機械学習モデル層。

* **ModelTrainer**   … 特徴量 DataFrame を受け取り、XGBoost で学習し MLflow に登録
* **ModelPredictor** … MLflow から指定 run_id / ステージのモデルをロードし推論

Example
-------
>>> from posms.models import ModelTrainer
>>> from posms.features import FeatureBuilder
>>> X, y = FeatureBuilder().build()
>>> run_id = ModelTrainer().train(X, y)
"""

from __future__ import annotations
import logging

# ロガー設定（他アプリで import しても警告が出ないように）
logging.getLogger("posms.models").addHandler(logging.NullHandler())

# トップレベル re-export
try:
    from .trainer import ModelTrainer  # noqa: F401
    from .predictor import ModelPredictor  # noqa: F401
except ModuleNotFoundError:
    # 実装ファイルが未完成でもパッケージ import でエラーにしない
    pass

__all__: list[str] = ["ModelTrainer", "ModelPredictor"]
