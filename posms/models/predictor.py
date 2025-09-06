"""
posms.models.predictor
======================

ModelPredictor
--------------
- MLflow Model Registry（models:/）または run_id（runs:/）からモデルをロード
- まず sklearn フレーバーで読み込み、ダメなら pyfunc → unwrap にフォールバック
- Registry が使えない/見つからない場合は、Experiment の最新 run に自動フォールバック
- XGBRegressor/sklearn 互換モデル or PyFuncModel で推論を実行
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import mlflow
import numpy as np
import pandas as pd

from .._mlflow import set_tracking_uri_zero_config

LOGGER = logging.getLogger(__name__)


class ModelPredictor:
    """
    Parameters
    ----------
    run_id : str | None
        直接 run_id を指定する場合。None ならステージ指定が優先。
    stage : str | None
        MLflow Model Registry のステージ名 (例: 'Production')。既定 'Production'。
        None の場合は Registry を使わず、Experiment の最新 run へ直接フォールバック。
    model_name : str
        モデル登録名。既定 'posms'。
    tracking_uri : str | None
        MLflow Tracking URI。None ならゼロ設定（<repo>/mlruns）。
    experiment : str
        フォールバック検索に使う Experiment 名。既定 'posms'。
    """

    def __init__(
        self,
        run_id: Optional[str] = None,
        stage: Optional[str] = "Production",
        model_name: str = "posms",
        tracking_uri: Optional[str] = None,
        experiment: str = "posms",
    ) -> None:
        set_tracking_uri_zero_config(tracking_uri)

        self._client = mlflow.tracking.MlflowClient()
        self._experiment_name = experiment

        # 優先 URI の決定
        if run_id:
            self.model_uri = f"runs:/{run_id}/model"
        elif stage:
            self.model_uri = f"models:/{model_name}/{stage}"
        else:
            # stage=None → 最初から最新 run へフォールバック
            self.model_uri = self._latest_run_uri(experiment)

        LOGGER.info("Loading model from MLflow URI: %s", self.model_uri)
        self.model = self._load_model(self.model_uri)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def predict(self, X: pd.DataFrame | np.ndarray) -> np.ndarray:
        """予測を返す。`X` は DataFrame / ndarray のいずれでも可。"""
        if not isinstance(X, pd.DataFrame):
            X = pd.DataFrame(X)
        preds = self.model.predict(X)
        LOGGER.debug("Predicted %d rows", len(preds))
        return np.asarray(preds).reshape(-1)

    def predict_single(self, X: Dict[str, Any]) -> float:
        """1 サンプルを dict で受け取り予測値を float で返す。"""
        df = pd.DataFrame([X])
        return float(self.predict(df)[0])

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _load_model(self, uri: str):
        """sklearn→pyfunc の順でロード。失敗したら最新 run にフォールバック。"""
        try:
            return mlflow.sklearn.load_model(uri)
        except Exception:  # noqa: BLE001
            try:
                return mlflow.pyfunc.load_model(uri).unwrap_python_model()  # type: ignore[return-value]
            except Exception:
                # Registry 不可 or 存在しない場合など → 最新 run へ
                fallback_uri = self._latest_run_uri(self._experiment_name)
                LOGGER.warning("Falling back to latest run: %s", fallback_uri)
                try:
                    return mlflow.sklearn.load_model(fallback_uri)
                except Exception:
                    return mlflow.pyfunc.load_model(fallback_uri).unwrap_python_model()  # type: ignore[return-value]

    def _latest_run_uri(self, experiment_name: str) -> str:
        """Experiment の最新 run から runs:/.../model を返す。見つからなければ例外。"""
        exp = self._client.get_experiment_by_name(experiment_name)
        if not exp:
            raise RuntimeError(f"Experiment not found: {experiment_name}")

        runs = self._client.search_runs(
            [exp.experiment_id],
            order_by=["attributes.start_time DESC"],  # ← ここが重要
            max_results=1,
        )
        if not runs:
            raise RuntimeError(f"No runs found in experiment: {experiment_name}")
        run_id = runs[0].info.run_id
        return f"runs:/{run_id}/model"


# ------------ 手動テスト（任意） ------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    try:
        predictor = ModelPredictor()
        dummy = pd.DataFrame(
            {
                "dow": [0],
                "dow_sin": [0.0],
                "dow_cos": [1.0],
                "is_holiday": [0],
                "is_after_holiday": [0],
                "is_after_after_holiday": [0],
                "month": [8],
                "season": [2],
                "lag_1": [12000],
                "lag_7": [11500],
                "rolling_mean_7": [11800],
                "is_new_year": [0],
                "is_obon": [1],
                "price_increase_flag": [0],
            }
        )
        print("Prediction:", predictor.predict(dummy))
    except Exception as exc:
        LOGGER.warning("Model load failed: %s", exc)
