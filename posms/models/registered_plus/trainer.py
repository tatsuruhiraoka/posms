# posms/model/registered_plus/trainer.py
from __future__ import annotations

from typing import Sequence, Tuple

import mlflow
import pandas as pd
import xgboost as xgb

from posms.models.registered_plus.tuner import get_best_params
from posms.models.registered_plus.metrics import calc_metrics
from posms.models.registered_plus.features import FEATURES_REGISTERED_PLUS, TARGET


def _split_xy(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    X = df[FEATURES_REGISTERED_PLUS].copy()
    y = df[TARGET].astype(float)
    return X, y


def train(
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    experiment_name: str = "posms",
    run_name: str = "registered_plus",
):
    """書留＋レターパックプラスモデルの学習を行い、学習済みモデルを返す。"""

    X_train, y_train = _split_xy(df_train)
    X_valid, y_valid = _split_xy(df_valid)

    best_params = get_best_params()

    # MLflow の実験を設定
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=run_name) as run:
        model = xgb.XGBRegressor(
            **best_params,
            objective="reg:squarederror",
            n_jobs=-1,
        )

        model.fit(
            X_train,
            y_train,
            eval_set=[(X_valid, y_valid)],
            verbose=False,
        )

        # 予測とメトリクス
        y_pred_valid = model.predict(X_valid)
        metrics = calc_metrics(y_valid, y_pred_valid)

        # MLflow にログ
        mlflow.log_params(best_params)
        for k, v in metrics.items():
            mlflow.log_metric(k, v)

        mlflow.xgboost.log_model(model, artifact_path="model")

        # run_id も返しておくと便利（YAML 管理など）
        return model, run.info.run_id
