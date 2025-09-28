# posms/models/predictor.py
"""
posms.models.predictor
======================

ModelPredictor
--------------
- MLflow Model Registry（models:/）または run_id（runs:/）からモデルをロード
- **pyfunc フレーバー → xgboost フレーバー → sklearn フレーバー**の順でフォールバック
- Registry が使えない/見つからない場合は、Experiment の最新 run に自動フォールバック
- 推論入力は pandas.DataFrame 推奨（列名保持）。ndarray も可（内部で DataFrame 化）
- 配達日ロジック（平日かつ祝日でない）に基づく繰越と千通丸めのポストプロセス関数を同梱
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from decimal import Decimal, ROUND_HALF_UP

import mlflow
import mlflow.pyfunc
import mlflow.xgboost
import mlflow.sklearn
import numpy as np
import pandas as pd
import xgboost as xgb
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

        # 優先 URI を決定
        if run_id:
            self.model_uri = f"runs:/{run_id}/model"
        elif stage:
            self.model_uri = f"models:/{model_name}/{stage}"
        else:
            # stage=None → 最初から最新 run へフォールバック
            self.model_uri = self._latest_run_uri(experiment)

        # ロード先（どれか1つがセットされる）
        self._pyfunc_model: Optional[mlflow.pyfunc.PyFuncModel] = None
        self._xgb_booster: Optional[xgb.Booster] = None
        self._sk_model: Optional[Any] = None

        LOGGER.info("Loading model from MLflow URI: %s", self.model_uri)
        self._load_model(self.model_uri)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def predict(self, X: pd.DataFrame | np.ndarray) -> np.ndarray:
        """予測を返す。`X` は DataFrame / ndarray のいずれでも可。"""
        X_df = X if isinstance(X, pd.DataFrame) else pd.DataFrame(X)
        
        #MLflow の入力スキーマ（float32）に合わせて数値・bool列を float32 に統一
        X_df = X_df.copy()
        num_bool_cols = X_df.select_dtypes(include=["number", "bool"]).columns
        if len(num_bool_cols) > 0:
        	# 一括キャスト（安全のため copy 後に代入）
        	X_df[num_bool_cols] = X_df[num_bool_cols].astype(np.float32)

        # 1) pyfunc: DataFrame そのまま predict できる（signature も活用）
        if self._pyfunc_model is not None:
            pred = self._pyfunc_model.predict(X_df)
            return np.asarray(pred).reshape(-1)

        # 2) xgboost: Booster の場合は DMatrix に変換して predict
        if self._xgb_booster is not None:
            dmat = xgb.DMatrix(X_df, missing=np.nan)
            # best_iteration までで予測（互換フォールバック付き）
            try:
                best_it = getattr(self._xgb_booster, "best_iteration", None)
                if best_it is not None:
                    return self._xgb_booster.predict(dmat, iteration_range=(0, int(best_it) + 1))
            except TypeError:
                ntree_limit = getattr(self._xgb_booster, "best_ntree_limit", None)
                if ntree_limit is not None:
                    return self._xgb_booster.predict(dmat, ntree_limit=int(ntree_limit))
            return self._xgb_booster.predict(dmat)

        # 3) sklearn: そのまま predict
        if self._sk_model is not None:
            pred = self._sk_model.predict(X_df)
            return np.asarray(pred).reshape(-1)

        raise RuntimeError("No loaded model is available for prediction.")

    def predict_single(self, X: Dict[str, Any]) -> float:
        """1 サンプルを dict で受け取り予測値を float で返す。"""
        return float(self.predict(pd.DataFrame([X]))[0])

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _load_model(self, uri: str) -> None:
        """
        **pyfunc → xgboost → sklearn** の順でロード。
        すべて失敗したら Experiment の最新 run にフォールバックして再試行。
        """
        # 1) pyfunc（最も互換性が高い）
        try:
            self._pyfunc_model = mlflow.pyfunc.load_model(uri)
            LOGGER.info("Loaded model as pyfunc.")
            return
        except Exception:
            self._pyfunc_model = None

        # 2) xgboost（Booster 直読み）
        try:
            self._xgb_booster = mlflow.xgboost.load_model(uri)
            LOGGER.info("Loaded model as xgboost Booster.")
            return
        except Exception:
            self._xgb_booster = None

        # 3) sklearn（古い run 向け）
        try:
            self._sk_model = mlflow.sklearn.load_model(uri)
            LOGGER.info("Loaded model as sklearn estimator.")
            return
        except Exception:
            self._sk_model = None

        # フォールバック：最新 run
        fallback_uri = self._latest_run_uri(self._experiment_name)
        LOGGER.warning("Falling back to latest run: %s", fallback_uri)

        # 再帰的に最新 run でロードを試みる（失敗時は例外）
        self._load_model(fallback_uri)

    def _latest_run_uri(self, experiment_name: str) -> str:
        """Experiment の最新 run から runs:/.../model を返す。見つからなければ例外。"""
        exp = self._client.get_experiment_by_name(experiment_name)
        if not exp:
            raise RuntimeError(f"Experiment not found: {experiment_name}")

        runs = self._client.search_runs(
            [exp.experiment_id],
            order_by=["attributes.start_time DESC"],
            max_results=1,
        )
        if not runs:
            raise RuntimeError(f"No runs found in experiment: {experiment_name}")
        run_id = runs[0].info.run_id
        return f"runs:/{run_id}/model"

    # ------------------- 配達ロジック補助 -------------------
    @staticmethod
    def _round_to_thousand_half_up(x: float) -> int:
        """千通単位の四捨五入（負値は 0 に丸め）"""
        if x <= 0:
            return 0
        return int((Decimal(str(x)) / Decimal("1000")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)) * 1000

    @staticmethod
    def _is_holiday(dt) -> bool:
        # pandas.Timestamp / datetime / date いずれにも対応
        d = dt.date() if hasattr(dt, "date") else dt
        return bool(jpholiday.is_holiday(d))

    @classmethod
    def _is_delivery_day(cls, dt) -> bool:
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
            s = raw[value_col] if value_col and value_col in raw.columns else raw.iloc[:, 0]
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

        return pd.DataFrame(rows, columns=["date","raw_pred","carry_in","deliver_pred","is_delivery_day"])


# ------------ 手動テスト（任意） ------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
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
