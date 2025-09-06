# posms/models/trainer.py
"""
posms.models.trainer
====================

ModelTrainer
------------
* XGBoost Regressor を学習
* MLflow に Metrics・Params・Model を記録（ゼロ設定：<repo>/mlruns）
* 終了時に run_id を返却。必要に応じて Model Registry へ登録（利用可能な場合）
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import mlflow
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor

from .._mlflow import set_tracking_uri_zero_config
from ._metrics import rmse as rmse_metric

LOGGER = logging.getLogger(__name__)  # basicConfig は呼び出し側に委ねる


class ModelTrainer:
    """
    Parameters
    ----------
    params : dict | None
        XGBoost ハイパーパラメータ。None の場合は既定値。
    experiment : str
        MLflow Experiment 名。未存在なら自動作成。
    tracking_uri : str | None
        MLflow トラッキング URI。None ならゼロ設定でローカル file ストアを使用。
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
        "eval_metric": "rmse",  # 早期終了/評価の一貫性
    }

    def __init__(
        self,
        params: Optional[Dict[str, Any]] = None,
        experiment: str = "posms",
        tracking_uri: Optional[str] = None,
    ) -> None:
        set_tracking_uri_zero_config(tracking_uri)
        mlflow.set_experiment(experiment)

        self.params = self.DEFAULT_PARAMS.copy()
        if params:
            self.params.update(params)

        LOGGER.info(
            "ModelTrainer initialized. experiment=%s, tracking_uri=%s",
            experiment,
            mlflow.get_tracking_uri(),
        )

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
        val_split: float = 0.2,
        es_rounds: int = 50,
    ) -> str:
        """学習を実行し、run_id を返す。"""
        model = XGBRegressor(**self.params)

        with mlflow.start_run(tags=tags) as run:
            LOGGER.info("Training XGBoost ...")

            # ---- 型最適化（速度/安定性）----
            Xf = X.astype(np.float32, copy=False)
            y_arr = np.asarray(y, dtype=np.float32).reshape(-1)

            # ---- 時系列バリデーション（末尾 split）----
            n = len(Xf)
            val_n = max(1, int(n * val_split))
            use_val = (val_n >= 5) and ((n - val_n) >= 10)

            if use_val:
                X_tr, X_val = Xf.iloc[:-val_n], Xf.iloc[-val_n:]
                y_tr, y_val = y_arr[:-val_n], y_arr[-val_n:]
                self._fit_with_compat(model, X_tr, y_tr, X_val, y_val, es_rounds)
            else:
                X_tr, y_tr = Xf, y_arr
                X_val = None
                self._fit_with_compat(model, X_tr, y_tr, None, None, es_rounds)

            # ---- メトリクス計算（train/val）----
            preds_tr = model.predict(X_tr)
            rmse_tr = rmse_metric(y_tr, preds_tr)
            mae_tr = mean_absolute_error(y_tr, preds_tr)

            metrics = {"rmse_train": float(rmse_tr), "mae_train": float(mae_tr)}
            if use_val and X_val is not None:
                preds_val = model.predict(X_val)
                rmse_val = rmse_metric(y_val, preds_val)  # type: ignore[arg-type]
                mae_val = mean_absolute_error(y_val, preds_val)  # type: ignore[arg-type]
                metrics.update({"rmse_val": float(rmse_val), "mae_val": float(mae_val)})

            LOGGER.info(
                "Done. RMSE(train)=%.3f, MAE(train)=%.3f%s",
                metrics["rmse_train"],
                metrics["mae_train"],
                (
                    f", RMSE(val)={metrics.get('rmse_val'):.3f}, MAE(val)={metrics.get('mae_val'):.3f}"
                    if "rmse_val" in metrics
                    else ""
                ),
            )

            # ---- ログ ----
            mlflow.log_params(self.params)
            mlflow.log_metrics(metrics)

            # 署名と入力例（列名固定 & 将来の推論安全性向上）
            try:
                from mlflow.models.signature import infer_signature

                X_example = (
                    X_tr.head(2)
                    if isinstance(X_tr, pd.DataFrame)
                    else pd.DataFrame(X_tr)[:2]
                )
                sig = infer_signature(X_example, model.predict(X_example))
                mlflow.sklearn.log_model(
                    model, artifact_path="model", signature=sig, input_example=X_example
                )
            except Exception:
                mlflow.sklearn.log_model(model, artifact_path="model")

            # 便利タグ（再現性に役立つ）
            try:
                from importlib import metadata as _md

                mlflow.set_tags(
                    {
                        "posms_version": _md.version("posms"),
                        "feature_columns": ",".join(map(str, X.columns)),
                    }
                )
            except Exception:
                pass

            run_id = run.info.run_id

        # Model Registry（file ストアでは未サポートのため例外→警告で握り）
        if auto_register:
            try:
                model_uri = f"runs:/{run_id}/model"
                LOGGER.info(
                    "Registering model to MLflow Model Registry → %s (%s)",
                    stage,
                    model_uri,
                )
                mv = mlflow.register_model(model_uri, "posms")
                client = mlflow.tracking.MlflowClient()
                client.transition_model_version_stage(
                    name=mv.name,
                    version=mv.version,
                    stage=stage,
                    archive_existing_versions=True,
                )
            except Exception as e:
                LOGGER.warning(
                    "Model Registry が利用できないため自動登録をスキップ: %s", e
                )

        return run_id

    # ----------------------------------------------------------------
    # Internals
    # ----------------------------------------------------------------
    @staticmethod
    def _fit_with_compat(
        model: XGBRegressor,
        X_tr: pd.DataFrame | np.ndarray,
        y_tr: np.ndarray,
        X_val: Optional[pd.DataFrame | np.ndarray],
        y_val: Optional[np.ndarray],
        es_rounds: int,
    ) -> None:
        """
        XGBoost のバージョン差を吸収しつつ学習を実行。
        1) early_stopping_rounds → 2) callbacks(EarlyStopping) → 3) なし
        """
        if X_val is None or y_val is None:
            # 検証を使わない場合は素直に学習
            try:
                model.fit(X_tr, y_tr, verbose=False)
            except TypeError:
                model.fit(X_tr, y_tr)
            return

        # 1) early_stopping_rounds
        try:
            model.fit(
                X_tr,
                y_tr,
                eval_set=[(X_val, y_val)],
                verbose=False,
                early_stopping_rounds=es_rounds,
            )
            return
        except TypeError:
            pass

        # 2) callbacks (EarlyStopping)
        try:
            from xgboost.callback import EarlyStopping

            try:
                model.fit(
                    X_tr,
                    y_tr,
                    eval_set=[(X_val, y_val)],
                    verbose=False,
                    callbacks=[EarlyStopping(rounds=es_rounds, save_best=True)],
                )
                return
            except TypeError:
                # さらに古い版：callbacks も受け取らない
                pass
        except Exception:
            # コールバックが無い/読み込めない場合
            pass

        # 3) なし（eval_set だけ許せば付与、それもダメなら完全に無し）
        try:
            model.fit(X_tr, y_tr, eval_set=[(X_val, y_val)], verbose=False)
        except TypeError:
            model.fit(X_tr, y_tr)
