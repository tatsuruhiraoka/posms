"""
posms.models.trainer
====================

ModelTrainer
------------

* XGBoost Regressor を学習
* MLflow に Metrics・Params・Model を記録
* 終了時に run_id を返却、任意で `Production` ステージへ自動登録

Example
-------
>>> from posms.features import FeatureBuilder
>>> from posms.models import ModelTrainer
>>> X, y = FeatureBuilder().build()
>>> run_id = ModelTrainer().train(X, y, auto_register=True)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import mlflow
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

LOGGER = logging.getLogger("posms.models.trainer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class ModelTrainer:
    """
    Parameters
    ----------
    params : dict | None
        XGBoost ハイパーパラメータ。None の場合はデフォルトセット。
    experiment : str
        MLflow Experiment 名。未存在なら自動作成。
    tracking_uri : str | None
        MLflow トラッキング URI。None なら env の MLFLOW_TRACKING_URI を利用。
    """

    DEFAULT_PARAMS: Dict[str, Any] = {
        "n_estimators": 300,
        "learning_rate": 0.1,
        "max_depth": 6,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "objective": "reg:squarederror",
        "random_state": 42,
        "tree_method": "hist",
    }

    def __init__(
        self,
        params: Optional[Dict[str, Any]] = None,
        experiment: str = "posms",
        tracking_uri: Optional[str] = None,
    ) -> None:
        # .env 読み込み (MLFLOW_TRACKING_URI 等)
        env_path = Path(__file__).resolve().parents[2] / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        elif os.getenv("MLFLOW_TRACKING_URI"):
            mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))

        mlflow.set_experiment(experiment)

        self.params = params or self.DEFAULT_PARAMS.copy()
        LOGGER.info("ModelTrainer initialized. experiment=%s", experiment)

    # ----------------------------------------------------------------
    # Public API
    # ----------------------------------------------------------------
    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series | np.ndarray,
        auto_register: bool = False,
        stage: str = "Production",
        tags: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        学習を実行し、run_id を返す。

        Parameters
        ----------
        X, y : 特徴量とターゲット
        auto_register : bool
            True の場合、モデルを指定ステージ (既定: Production) に登録
        stage : str
            MLflow Model Registry のステージ名
        tags : dict
            MLflow run に付与する任意タグ
        """
        model = XGBRegressor(**self.params)

        with mlflow.start_run(tags=tags) as run:
            LOGGER.info("Training XGBoost ...")
            model.fit(X, y)

            preds = model.predict(X)
            rmse = mean_squared_error(y, preds, squared=False)
            mae = mean_absolute_error(y, preds)

            LOGGER.info("Training finished. RMSE=%.2f, MAE=%.2f", rmse, mae)

            # MLflow へログ
            mlflow.log_params(self.params)
            mlflow.log_metrics({"rmse": rmse, "mae": mae})
            mlflow.sklearn.log_model(model, artifact_path="model")

            run_id = run.info.run_id

        # モデルレジストリに登録
        if auto_register:
            model_uri = f"runs:/{run_id}/model"
            LOGGER.info("Registering model to MLflow Model Registry → %s (%s)", stage, model_uri)
            mv = mlflow.register_model(model_uri, "posms")
            # Transition to target stage
            client = mlflow.tracking.MlflowClient()
            client.transition_model_version_stage(
                name=mv.name,
                version=mv.version,
                stage=stage,
                archive_existing_versions=True,
            )

        return run_id
