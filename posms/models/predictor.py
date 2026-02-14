# posms/models/predictor.py
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
    Unified MLflow ModelPredictor.

    - runs:/<run_id>/model が最優先（確実）
    - それ以外は Experiment の最新 run を tags で絞って取得（事故防止）
    - pyfunc → xgboost → sklearn の順でロードを試みる
    """

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        tracking_uri: Optional[str] = None,
        experiment: str = "posms",
        model_name: Optional[str] = None,
        mail_kind: Optional[str] = None,
        office_id: Optional[int] = None,
        stage: Optional[str] = None,  # 将来廃止方向なので基本は None 推奨
    ) -> None:
        set_tracking_uri_zero_config(tracking_uri)

        self._client = mlflow.tracking.MlflowClient()
        self._experiment_name = experiment

        self._pyfunc_model: Optional[mlflow.pyfunc.PyFuncModel] = None
        self._xgb_booster: Optional[xgb.Booster] = None
        self._sk_model: Optional[Any] = None

        self._mail_kind = (mail_kind or "").lower() or None
        self._office_id = office_id
        self._model_name = (model_name or "").strip() or None
        self.model_uri: str

        # -------------------------------
        # モデル URI を決定
        # -------------------------------
        uris: list[str] = []
        if run_id:
            uris = [f"runs:/{run_id}/model"]
        else:
            # stage は廃止方向なので基本は使わない（必要なら指定可）
            if stage:
                uris.append(f"models:/{model_name}/{stage}")
            uris.append(self._latest_run_uri(experiment))

        last_err: Optional[Exception] = None
        loaded_uri: Optional[str] = None

        for uri in uris:
            LOGGER.info("Trying MLflow model: %s", uri)
            try:
                self._load_model_from_uri(uri)
                loaded_uri = uri
                break
            except Exception as e:  # noqa: BLE001
                LOGGER.warning("Failed to load model from %s: %s", uri, e)
                last_err = e

        if loaded_uri is None:
            raise RuntimeError(f"モデルをロードできませんでした: {uris}") from last_err

        self.model_uri = loaded_uri
        LOGGER.info("Model loaded successfully from: %s", self.model_uri)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def predict(self, X: pd.DataFrame | np.ndarray | Dict[str, Any]) -> np.ndarray:
        """予測を返す。`X` は DataFrame / ndarray / dict のいずれでも可。"""
        if isinstance(X, pd.DataFrame):
            X_df = X.copy()
        elif isinstance(X, dict):
            X_df = pd.DataFrame([X])
        else:
            X_df = pd.DataFrame(X)

        # 数値・bool列を float32 に統一
        num_cols = X_df.select_dtypes(include=["number", "bool"]).columns
        if len(num_cols) > 0:
            X_df[num_cols] = X_df[num_cols].astype(np.float32)

        # 1) pyfunc
        if self._pyfunc_model is not None:
            pred = self._pyfunc_model.predict(X_df)
            return np.asarray(pred).reshape(-1)

        # 2) xgboost Booster
        if self._xgb_booster is not None:
            dmat = xgb.DMatrix(X_df, missing=np.nan)
            try:
                best_it = getattr(self._xgb_booster, "best_iteration", None)
                if best_it is not None:
                    return self._xgb_booster.predict(dmat, iteration_range=(0, int(best_it) + 1))
            except TypeError:
                ntree_limit = getattr(self._xgb_booster, "best_ntree_limit", None)
                if ntree_limit is not None:
                    return self._xgb_booster.predict(dmat, ntree_limit=int(ntree_limit))
            return self._xgb_booster.predict(dmat)

        # 3) sklearn
        if self._sk_model is not None:
            pred = self._sk_model.predict(X_df)
            return np.asarray(pred).reshape(-1)

        raise RuntimeError("No loaded model is available for prediction.")

    def predict_single(self, X: Dict[str, Any]) -> float:
        return float(self.predict(pd.DataFrame([X]))[0])

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _load_model_from_uri(self, uri: str) -> None:
        """指定された URI から pyfunc → xgboost → sklearn の順でロード。"""
        self._pyfunc_model = None
        self._xgb_booster = None
        self._sk_model = None

        last_err: Optional[Exception] = None

        # 1) pyfunc
        try:
            self._pyfunc_model = mlflow.pyfunc.load_model(uri)
            LOGGER.info("Loaded model as pyfunc.")
            return
        except Exception as e:
            last_err = e
            self._pyfunc_model = None

        # 2) xgboost
        try:
            self._xgb_booster = mlflow.xgboost.load_model(uri)
            LOGGER.info("Loaded model as xgboost Booster.")
            return
        except Exception as e:
            last_err = e
            self._xgb_booster = None

        # 3) sklearn
        try:
            self._sk_model = mlflow.sklearn.load_model(uri)
            LOGGER.info("Loaded model as sklearn estimator.")
            return
        except Exception as e:
            last_err = e
            self._sk_model = None

        raise RuntimeError(f"Failed to load model from URI: {uri}") from last_err

    def _latest_run_uri(self, experiment_name: str) -> str:
        """Experiment の最新 run から runs:/.../model を返す（tags で絞る）。"""
        exp = self._client.get_experiment_by_name(experiment_name)
        if not exp:
            raise RuntimeError(f"Experiment not found: {experiment_name}")

        filters = []
        if self._mail_kind:
            filters.append(f"tags.mail_kind = '{self._mail_kind}'")
        if self._office_id is not None:
            filters.append(f"tags.office_id = '{self._office_id}'")
        if self._model_name:
            filters.append(f"tags.model_name = '{self._model_name}'")

        filter_string = " and ".join(filters) if filters else ""

        runs = self._client.search_runs(
            [exp.experiment_id],
            filter_string=filter_string,
            order_by=["attributes.start_time DESC"],
            max_results=1,
        )
        if not runs:
            raise RuntimeError(
                f"No runs found in experiment={experiment_name} filter={filter_string!r}"
            )

        run_id = runs[0].info.run_id
        return f"runs:/{run_id}/model"

    # ------------------- 配達ロジック補助（必要なら残す） -------------------
    @staticmethod
    def _round_to_thousand_half_up(x: float) -> int:
        if x <= 0:
            return 0
        return (
            int(
                (Decimal(str(x)) / Decimal("1000")).quantize(
                    Decimal("1"), rounding=ROUND_HALF_UP
                )
            )
            * 1000
        )

    @staticmethod
    def _is_holiday(dt) -> bool:
        d = dt.date() if hasattr(dt, "date") else dt
        return bool(jpholiday.is_holiday(d))

    @classmethod
    def _is_delivery_day(cls, dt) -> bool:
        d = dt if hasattr(dt, "weekday") else pd.to_datetime(dt)
        return (d.weekday() < 5) and (not cls._is_holiday(d))

    @classmethod
    def apply_delivery_rules(
        cls,
        raw: "pd.Series | pd.DataFrame",
        *,
        value_col: str | None = None,
        round_to_thousand: bool = True,
        extend_to_next_delivery: bool = True,
    ) -> "pd.DataFrame":
        if isinstance(raw, pd.DataFrame):
            s = raw[value_col] if value_col and value_col in raw.columns else raw.iloc[:, 0]
        else:
            s = raw
        s = s.asfreq("D")

        carry = 0.0
        rows = []
        for dt, val in s.items():
            v = float(val)
            if cls._is_delivery_day(dt):
                delivered = v + carry
                deliver = cls._round_to_thousand_half_up(delivered) if round_to_thousand else max(0.0, delivered)
                rows.append((dt.date(), v, carry if carry > 0 else None, int(max(0, deliver)), True))
                carry = 0.0
            else:
                carry += v
                rows.append((dt.date(), v, None, 0, False))

        if extend_to_next_delivery and carry > 0:
            dt = s.index[-1] + pd.Timedelta(days=1)
            while not cls._is_delivery_day(dt):
                dt += pd.Timedelta(days=1)
            deliver = cls._round_to_thousand_half_up(carry) if round_to_thousand else max(0.0, carry)
            rows.append((dt.date(), 0.0, carry, int(max(0, deliver)), True))

        return pd.DataFrame(rows, columns=["date", "raw_pred", "carry_in", "deliver_pred", "is_delivery_day"])
