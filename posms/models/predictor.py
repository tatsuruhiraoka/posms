"""
posms.models.predictor
======================

ModelPredictor
--------------

- MLflow Model Registry または run_id からモデルをロード
- XGBRegressor (scikit‑learn API) として推論を実行
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
from xgboost import XGBRegressor

LOGGER = logging.getLogger("posms.models.predictor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class ModelPredictor:
    """
    Parameters
    ----------
    run_id : str | None
        直接 run_id を指定する場合。None ならステージ指定が優先。
    stage : str | None
        MLflow Model Registry のステージ名 (e.g. 'Production')。既定 'Production'。
    model_name : str
        モデル登録名。既定 'posms'。
    tracking_uri : str | None
        MLflow Tracking URI。None なら環境変数を使用。
    """

    def __init__(
        self,
        run_id: Optional[str] = None,
        stage: Optional[str] = "Production",
        model_name: str = "posms",
        tracking_uri: Optional[str] = None,
    ) -> None:
        # 環境変数ロード
        env_path = Path(__file__).resolve().parents[2] / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        elif os.getenv("MLFLOW_TRACKING_URI"):
            mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))

        self.model_uri: str
        if run_id:
            self.model_uri = f"runs:/{run_id}/model"
        else:
            self.model_uri = f"models:/{model_name}/{stage}"

        LOGGER.info("Loading model from MLflow URI: %s", self.model_uri)
        # unwrap_python_model で XGBRegressor を直接取得
        self.model: XGBRegressor = (
            mlflow.pyfunc.load_model(self.model_uri).unwrap_python_model()  # type: ignore
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def predict(self, X: pd.DataFrame | np.ndarray) -> np.ndarray:
        """
        Parameters
        ----------
        X : pandas.DataFrame | numpy.ndarray
            予測用特徴量

        Returns
        -------
        numpy.ndarray
            予測結果 (1D)
        """
        preds = self.model.predict(X)
        LOGGER.debug("Predicted %d rows", len(preds))
        return preds

    def predict_single(self, X: Dict[str, Any]) -> float:
        """
        1 サンプルを dict で受け取り予測値を float で返す
        """
        df = pd.DataFrame([X])
        return float(self.predict(df)[0])


# ------------- CLI テスト ----------------
if __name__ == "__main__":
    # 簡易テスト: 空 DataFrame で shape 確認
    try:
        predictor = ModelPredictor()
        dummy = pd.DataFrame(
            {
                "weekday": [0],
                "month": [8],
                "day": [1],
                "lag_1": [12000],
                "lag_7": [11500],
                "roll_mean_7": [11800],
                "is_holiday": [0],
                "price_increase_flag": [0],
            }
        )
        print("Prediction:", predictor.predict(dummy))
    except Exception as exc:
        LOGGER.warning("Model load failed: %s", exc)
