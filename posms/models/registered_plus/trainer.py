# posms/models/registered_plus/trainer.py
from __future__ import annotations

from typing import Tuple, Dict, Optional, Any

import mlflow
import numpy as np
import pandas as pd
import xgboost as xgb
from mlflow.models.signature import infer_signature

from posms.models.registered_plus.tuner import get_best_params
from posms.models.registered_plus.metrics import calc_metrics
from posms.models.registered_plus.features import FEATURES_REGISTERED_PLUS, TARGET


def _split_xy(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    X = df[list(FEATURES_REGISTERED_PLUS)].copy()
    y = df[TARGET].astype(float)
    return X, y


def train(
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    experiment_name: str = "posms",
    run_name: str = "registered_plus",
    *,
    model_name: str = "posms_registered_plus",
    tags: Optional[Dict[str, Any]] = None,
) -> Tuple[xgb.XGBRegressor, str]:
    """
    registered_plus（書留＋レタパプラス sum 系特徴量）モデルの学習を行い、(model, run_id) を返す。

    重要:
    - signature を FEATURES_REGISTERED_PLUS で保存する（推論時の列ズレ事故を防ぐ）
    - tags に mail_kind/office_id/feature_set 等を刻む（run探索のフィルタに使う）
    """
    X_train, y_train = _split_xy(df_train)
    X_valid, y_valid = _split_xy(df_valid)

    best_params = get_best_params()

    # MLflow の実験を設定
    mlflow.set_experiment(experiment_name)

    # run に刻むタグ（最低限はここで保証）
    base_tags: Dict[str, str] = {
        "model_name": str(model_name),
        "mail_kind": "registered_plus",
        "feature_set": ",".join(map(str, FEATURES_REGISTERED_PLUS)),
    }
    if tags:
        # 値は MLflow tags の制約上 string 化
        base_tags.update({str(k): str(v) for k, v in tags.items()})

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.set_tags(base_tags)

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
            mlflow.log_metric(str(k), float(v))

        # signature / input_example を付けて保存（列ズレ事故防止）
        try:
            X_example = X_train.head(2).astype(np.float32, copy=False)
            y_example = model.predict(X_example)
            sig = infer_signature(X_example, y_example)

            mlflow.xgboost.log_model(
                model,
                artifact_path="model",
                signature=sig,
                input_example=X_example,
            )
        except Exception:
            # 万一失敗しても学習が止まらないように最低限の保存は行う
            mlflow.xgboost.log_model(model, artifact_path="model")

        return model, run.info.run_id
