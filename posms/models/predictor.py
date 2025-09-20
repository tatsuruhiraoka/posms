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
from decimal import Decimal, ROUND_HALF_UP
import mlflow
import numpy as np
import pandas as pd
import jpholiday
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
        
    @staticmethod
    def _round_to_thousand_half_up(x: float) -> int:
        """千通単位の四捨五入（負値は 0 に丸め）"""
        if x <= 0:
            return 0
        return int((Decimal(str(x)) / Decimal("1000")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)) * 1000

    @staticmethod
    def _is_holiday(dt):
        # pandas.Timestamp / datetime / date いずれにも対応
        d = dt.date() if hasattr(dt, "date") else dt
        return bool(jpholiday.is_holiday(d))

    @classmethod
    def _is_delivery_day(cls, dt):
        """配達日= 平日かつ祝日でない"""
        return (dt.weekday() < 5) and (not cls._is_holiday(dt))

    @classmethod
    def apply_delivery_rules(
        cls,
        raw: "pd.Series | pd.DataFrame",
        *,
        value_col: str | None = None,
        round_to_thousand: bool = True,
        extend_to_next_delivery: bool = True,
    ) -> "pd.DataFrame":
        """
        生の“日次予測”に、土日祝の繰り越し＆千通丸めを適用するだけのポストプロセス。
        ここでは“再予測”は行いません。

        Parameters
        ----------
        raw : pd.Series | pd.DataFrame
            日付を DatetimeIndex に持つ日次予測。
            - Series の場合: 値が生予測
            - DataFrame の場合: `value_col` で列名を指定（未指定なら先頭列）
        value_col : str | None
            DataFrame の場合の列名
        round_to_thousand : bool
            True なら千通単位（四捨五入）に丸めて配達日に計上
        extend_to_next_delivery : bool
            期間末尾が非配達日のとき、次の配達日（期間外）に繰り越し分を1行追加

        Returns
        -------
        pd.DataFrame ・・・ columns = ["date","raw_pred","carry_in","deliver_pred","is_delivery_day"]
        """
        # 入力を Series に正規化（D 日次に揃える）
        if isinstance(raw, pd.DataFrame):
            s = raw[value_col] if value_col in raw.columns else raw.iloc[:, 0]
        else:
            s = raw
        s = s.asfreq("D")

        carry = 0.0
        rows = []
        for dt, val in s.items():
            val = float(val)
            if cls._is_delivery_day(dt):
                delivered = val + carry
                deliver = cls._round_to_thousand_half_up(delivered) if round_to_thousand else delivered
                rows.append((dt.date(), val, carry if carry > 0 else None, int(max(0, deliver)), True))
                carry = 0.0
            else:
                carry += val
                rows.append((dt.date(), val, None, 0, False))

        # 末尾が非配達日で carry が残れば、次の配達日に繰り越し（期間外1行を追加）
        if extend_to_next_delivery and carry > 0:
            dt = s.index[-1] + pd.Timedelta(days=1)
            while not cls._is_delivery_day(dt):
                dt += pd.Timedelta(days=1)
            deliver = cls._round_to_thousand_half_up(carry) if round_to_thousand else carry
            rows.append((dt.date(), 0.0, carry, int(max(0, deliver)), True))
            carry = 0.0

        return pd.DataFrame(rows, columns=["date","raw_pred","carry_in","deliver_pred","is_delivery_day"])


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
