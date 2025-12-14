# posms/models/trainer.py
"""
posms.models.trainer
====================

ModelTrainer (DMatrix + xgb.cv 版)
----------------------------------
* XGBoost Booster を DMatrix で学習（純正API: xgb.cv / xgb.train）
* xgb.cv で最適ブースト回数（best_nrounds = idxmin() + 1）を確定
* MLflow に Metrics・Params・Model を記録（ゼロ設定：<repo>/mlruns）
* 代表的なグラフ/CSV を Artifacts に保存：
    - pred_vs_actual_val.png
    - residuals_hist_val.png
    - learning_curve_rmse.png（evals_result_ が得られた場合）
    - feature_importance.png（上位特徴量）
    - val_predictions.csv（検証の y_true / y_pred / residual）
* 終了時に run_id を返却。必要に応じて Model Registry へ登録（利用可能な場合）

メモ:
- 率の指標は MAPE の代わりに sMAPE / WAPE を採用（ゼロ割りの不安定回避）
- 検証は従来どおり「末尾 hold-out（val_split）」で計測・可視化
- パラメータは sklearn 互換（learning_rate, n_estimators 等）を受け取り、
  xgb.train 用に必要なキー（eta, seed 等）へ内部変換して利用
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import mlflow
import mlflow.xgboost  # noqa: F401  # モデル保存に利用
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import TimeSeriesSplit

from .._mlflow import set_tracking_uri_zero_config
from ._metrics import rmse as rmse_metric  # 既存の rmse 実装を利用

LOGGER = logging.getLogger(__name__)  # basicConfig は呼び出し側に委ねる


def _smape(y: np.ndarray, yhat: np.ndarray) -> float:
    """対称 MAPE（0〜200%）。ゼロ割り回避。"""
    y = np.asarray(y, dtype=float).reshape(-1)
    yhat = np.asarray(yhat, dtype=float).reshape(-1)
    denom = (np.abs(y) + np.abs(yhat)) / 2.0
    return float(100.0 * np.mean(np.abs(y - yhat) / (denom + 1e-8)))


def _wape(y: np.ndarray, yhat: np.ndarray) -> float:
    """加重平均絶対誤差率。ゼロ割り回避。"""
    y = np.asarray(y, dtype=float).reshape(-1)
    yhat = np.asarray(yhat, dtype=float).reshape(-1)
    return float(100.0 * np.sum(np.abs(y - yhat)) / (np.sum(np.abs(y)) + 1e-8))


class ModelTrainer:
    """
    Parameters
    ----------
    params : dict | None
        XGBoost のハイパーパラメータ（sklearn 互換キーで可）。
        例: learning_rate, max_depth, subsample, colsample_bytree, eval_metric, random_state など
    experiment : str
        MLflow Experiment 名。未存在なら自動作成。
    tracking_uri : str | None
        MLflow トラッキング URI。None ならゼロ設定でローカル file ストアを使用。
    cv_n_splits : int
        xgb.cv に渡す TimeSeriesSplit の分割数。
    cv_max_rounds : int
        xgb.cv の最大ブースト回数（early_stopping で自動短縮）。
    """

    DEFAULT_PARAMS: Dict[str, Any] = {
        # sklearn 互換キーで受け取り、内部で xgb.train 用に変換します
        "n_estimators": 300,
        "learning_rate": 0.1,  # → eta
        "max_depth": 6,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "objective": "reg:squarederror",
        "random_state": 42,  # → seed
        "tree_method": "hist",
        "eval_metric": "rmse",
    }

    def __init__(
        self,
        params: Optional[Dict[str, Any]] = None,
        experiment: str = "posms",
        tracking_uri: Optional[str] = None,
        cv_n_splits: int = 5,
        cv_max_rounds: int = 5000,
    ) -> None:
        set_tracking_uri_zero_config(tracking_uri)
        mlflow.set_experiment(experiment)

        self.params = self.DEFAULT_PARAMS.copy()
        if params:
            self.params.update(params)

        self.cv_n_splits = int(cv_n_splits)
        self.cv_max_rounds = int(cv_max_rounds)

        LOGGER.info(
            "ModelTrainer initialized. experiment=%s, tracking_uri=%s, cv_n_splits=%d, cv_max_rounds=%d",
            experiment,
            mlflow.get_tracking_uri(),
            self.cv_n_splits,
            self.cv_max_rounds,
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
        es_rounds: int = 200,
    ) -> str:
        """
        学習を実行し、run_id を返す。MLflow に Metrics と（可能なら）グラフ/CSV も記録する。

        フロー:
          1) xgb.cv（TimeSeriesSplit）で best_nrounds を推定（idxmin()+1）
          2) 末尾 hold-out で最終学習（xgb.train + early_stopping）
          3) train/val の指標を記録（RMSE/MAE/R2/sMAPE/WAPE）
        """
        with mlflow.start_run(tags=tags) as run:
            LOGGER.info("Training XGBoost (DMatrix + xgb.cv) ...")

            # ---- 型最適化（速度/安定性）----
            Xf: pd.DataFrame = X.astype(np.float32, copy=False)
            y_arr = np.asarray(y, dtype=np.float32).reshape(-1)

            # ---- xgb.cv で最良ラウンド決定 ----
            d_all = xgb.DMatrix(Xf, label=y_arr)  # pandas -> 列名も保持される
            folds = list(TimeSeriesSplit(n_splits=self.cv_n_splits).split(Xf))
            xgb_params = self._to_xgb_params(self.params)

            cv_df = xgb.cv(
                params=xgb_params,
                dtrain=d_all,
                num_boost_round=self.cv_max_rounds,
                folds=folds,
                early_stopping_rounds=max(1, int(es_rounds)),
                metrics=("rmse",),
                seed=xgb_params.get("seed", 42),
                verbose_eval=False,
            )
            best_nrounds = int(cv_df["test-rmse-mean"].idxmin()) + 1  # ★重要
            rmse_cv_min = float(cv_df["test-rmse-mean"].min())
            rmse_cv_std = float(cv_df["test-rmse-std"].iloc[best_nrounds - 1])

            # ---- hold-out を切って最終学習 ----
            n = len(Xf)
            val_n = max(1, int(n * val_split))
            use_val = (val_n >= 5) and ((n - val_n) >= 10)

            if use_val:
                X_tr, X_val = Xf.iloc[:-val_n], Xf.iloc[-val_n:]
                y_tr, y_val = y_arr[:-val_n], y_arr[-val_n:]
                d_tr = xgb.DMatrix(X_tr, label=y_tr)
                d_val = xgb.DMatrix(X_val, label=y_val)
                evals = [(d_tr, "train"), (d_val, "valid")]
                # 若干余裕を持たせて早期終了で調整
                num_round = best_nrounds + es_rounds
                booster = xgb.train(
                    params=xgb_params,
                    dtrain=d_tr,
                    num_boost_round=num_round,
                    evals=evals,
                    early_stopping_rounds=es_rounds,
                    verbose_eval=False,
                )
                used_rounds = int(getattr(booster, "best_iteration", best_nrounds)) + 1
            else:
                d_tr = xgb.DMatrix(Xf, label=y_arr)
                evals = [(d_tr, "train")]
                num_round = best_nrounds + es_rounds
                booster = xgb.train(
                    params=xgb_params,
                    dtrain=d_tr,
                    num_boost_round=num_round,
                    evals=evals,
                    early_stopping_rounds=es_rounds,
                    verbose_eval=False,
                )
                used_rounds = int(getattr(booster, "best_iteration", best_nrounds)) + 1
                X_val = None
                y_val = None

            # ---- 予測（best_iteration を尊重）----
            preds_tr = self._predict_booster(booster, d_tr, used_rounds)
            rmse_tr = rmse_metric(y_tr if use_val else y_arr, preds_tr)
            mae_tr = mean_absolute_error(y_tr if use_val else y_arr, preds_tr)
            r2_tr = r2_score(y_tr if use_val else y_arr, preds_tr)
            smape_tr = _smape(y_tr if use_val else y_arr, preds_tr)
            wape_tr = _wape(y_tr if use_val else y_arr, preds_tr)

            metrics: Dict[str, float] = {
                "cv_rmse_mean_min": rmse_cv_min,
                "cv_rmse_std_at_min": rmse_cv_std,
                "best_nrounds": float(best_nrounds),
                "used_rounds": float(used_rounds),
                "rmse_train": float(rmse_tr),
                "mae_train": float(mae_tr),
                "r2_train": float(r2_tr),
                "smape_train": float(smape_tr),
                "wape_train": float(wape_tr),
            }

            preds_val = None
            if use_val and X_val is not None:
                d_val = xgb.DMatrix(X_val, label=y_val)  # type: ignore[arg-type]
                preds_val = self._predict_booster(booster, d_val, used_rounds)
                rmse_val = rmse_metric(y_val, preds_val)  # type: ignore[arg-type]
                mae_val = mean_absolute_error(y_val, preds_val)  # type: ignore[arg-type]
                r2_val = r2_score(y_val, preds_val)  # type: ignore[arg-type]
                smape_val = _smape(y_val, preds_val)  # type: ignore[arg-type]
                wape_val = _wape(y_val, preds_val)  # type: ignore[arg-type]
                metrics.update(
                    {
                        "rmse_val": float(rmse_val),
                        "mae_val": float(mae_val),
                        "r2_val": float(r2_val),
                        "smape_val": float(smape_val),
                        "wape_val": float(wape_val),
                    }
                )

            LOGGER.info(
                "Done. CV RMSE(min)=%.3f @%d, Used rounds=%d | RMSE(train)=%.3f, MAE(train)=%.3f, R2(train)=%.3f%s",
                metrics["cv_rmse_mean_min"],
                int(metrics["best_nrounds"]),
                int(metrics["used_rounds"]),
                metrics["rmse_train"],
                metrics["mae_train"],
                metrics["r2_train"],
                (
                    f", RMSE(val)={metrics.get('rmse_val'):.3f}, MAE(val)={metrics.get('mae_val'):.3f}, "
                    f"R2(val)={metrics.get('r2_val'):.3f}"
                    if "rmse_val" in metrics
                    else ""
                ),
            )

            # ---- ログ ----
            # 使ったパラメータ（sklearn互換 + xgb.train 実投影）を両方残す
            mlflow.log_params(self.params)
            mlflow.log_params(self._to_xgb_params(self.params, log_safe=True))
            mlflow.log_metrics(metrics)

            # ---- 可視化/CSV（matplotlib 等が無ければ静かにスキップ）----
            outdir = Path("/tmp/posms_train")
            outdir.mkdir(parents=True, exist_ok=True)
            try:
                import matplotlib.pyplot as plt  # optional

                if use_val and X_val is not None and preds_val is not None:
                    # 1) 予測 vs 実測（検証）
                    fig = plt.figure(figsize=(5.6, 4.2))
                    ax = fig.add_subplot(111)
                    ax.scatter(y_val, preds_val, s=12)  # type: ignore[arg-type]
                    lo = float(min(np.min(y_val), np.min(preds_val)))  # type: ignore[arg-type]
                    hi = float(max(np.max(y_val), np.max(preds_val)))  # type: ignore[arg-type]
                    ax.plot([lo, hi], [lo, hi], linestyle="--")
                    ax.set_xlabel("Actual (val)")
                    ax.set_ylabel("Predicted (val)")
                    ax.set_title("Predicted vs Actual (Validation)")
                    fig.tight_layout()
                    fig.savefig(outdir / "pred_vs_actual_val.png", dpi=160)
                    plt.close(fig)

                    # 2) 残差ヒスト（検証）
                    fig = plt.figure(figsize=(5.6, 4.2))
                    ax = fig.add_subplot(111)
                    ax.hist((y_val - preds_val), bins=30)  # type: ignore[operator]
                    ax.set_xlabel("Residual (val)")
                    ax.set_ylabel("Count")
                    ax.set_title("Residuals Histogram (Validation)")
                    fig.tight_layout()
                    fig.savefig(outdir / "residuals_hist_val.png", dpi=160)
                    plt.close(fig)

                # 3) 学習曲線（evals_result_ があれば）
                try:
                    ev = booster.evals_result()
                    key_train = "train"
                    key_valid = "valid" if use_val else None
                    if key_train in ev and "rmse" in ev[key_train]:
                        fig = plt.figure(figsize=(6.0, 4.0))
                        ax = fig.add_subplot(111)
                        ax.plot(ev[key_train]["rmse"], label="train")
                        if key_valid and key_valid in ev and "rmse" in ev[key_valid]:
                            ax.plot(ev[key_valid]["rmse"], label="valid")
                        ax.set_xlabel("Iteration")
                        ax.set_ylabel("RMSE")
                        ax.set_title("Learning Curve (RMSE)")
                        ax.legend()
                        fig.tight_layout()
                        fig.savefig(outdir / "learning_curve_rmse.png", dpi=160)
                        plt.close(fig)
                except Exception:
                    pass

                # 4) 特徴量重要度（上位30）
                try:
                    imp_dict = booster.get_score(importance_type="gain")
                    # DataFrame から作った DMatrix なら列名が入る想定
                    cols = list(Xf.columns)
                    imp = pd.Series({c: imp_dict.get(c, 0.0) for c in cols}).sort_values(ascending=False).head(30)
                    fig = plt.figure(figsize=(6.2, 6.8))
                    ax = fig.add_subplot(111)
                    imp[::-1].plot(kind="barh", ax=ax)
                    ax.set_title("Feature Importance (Top 30, gain)")
                    ax.set_xlabel("Importance")
                    fig.tight_layout()
                    fig.savefig(outdir / "feature_importance.png", dpi=160)
                    plt.close(fig)
                except Exception:
                    pass
            except Exception:
                # matplotlib 未導入などは静かにスキップ
                pass

            # 検証予測 CSV
            try:
                if use_val and X_val is not None and preds_val is not None:
                    val_df = pd.DataFrame(
                        {
                            "y_true": y_val,  # type: ignore[arg-type]
                            "y_pred": preds_val,
                            "residual": (y_val - preds_val),  # type: ignore[operator]
                        }
                    )
                    val_df.to_csv(outdir / "val_predictions.csv", index=False)
            except Exception:
                pass

            # 署名と入力例（列名固定 & 将来の推論安全性向上）
            try:
                from mlflow.models.signature import infer_signature

                X_example = Xf.head(2)
                d_example = xgb.DMatrix(X_example)
                y_example = self._predict_booster(booster, d_example, used_rounds)
                sig = infer_signature(X_example, y_example)
                mlflow.xgboost.log_model(  # Booster をそのまま保存
                    booster, artifact_path="model", signature=sig, input_example=X_example
                )
            except Exception:
                # 署名取得に失敗した場合は素朴に保存
                mlflow.xgboost.log_model(booster, artifact_path="model")

            # 便利タグ（再現性に役立つ）
            try:
                from importlib import metadata as _md

                mlflow.set_tags(
                    {
                        "posms_version": _md.version("posms"),
                        "feature_columns": ",".join(map(str, X.columns)),
                        "cv_n_splits": str(self.cv_n_splits),
                        "cv_max_rounds": str(self.cv_max_rounds),
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
    def _to_xgb_params(params: Dict[str, Any], *, log_safe: bool = False) -> Dict[str, Any]:
        """
        sklearn 互換パラメータを xgb.train 用に変換。
        log_safe=True の場合は学習に無関係なキーを落として見やすく整形。
        """
        p = dict(params)
        # エイリアス変換
        if "learning_rate" in p:
            p["eta"] = float(p.pop("learning_rate"))
        if "random_state" in p:
            p["seed"] = int(p.pop("random_state"))
        # sklearn専用キーは削除
        p.pop("n_estimators", None)
        # 既定の eval_metric が無ければ rmse
        p.setdefault("eval_metric", "rmse")
        if log_safe:
            # ログ用に順序・冗長削減（任意）
            keys = [
                "objective", "eta", "max_depth", "subsample", "colsample_bytree",
                "gamma", "min_child_weight", "alpha", "lambda",
                "tree_method", "eval_metric", "seed",
            ]
            return {k: p[k] for k in keys if k in p}
        return p

    @staticmethod
    def _predict_booster(booster: xgb.Booster, dmat: xgb.DMatrix, used_rounds: int) -> np.ndarray:
        """
        xgboost のバージョン差を吸収して、best_iteration までで予測。
        """
        try:
            return booster.predict(dmat, iteration_range=(0, int(used_rounds)))
        except TypeError:
            # 古い版は iteration_range を受け取らない
            try:
                ntree_limit = getattr(booster, "best_ntree_limit", None)
                if ntree_limit is not None:
                    return booster.predict(dmat, ntree_limit=int(ntree_limit))
            except Exception:
                pass
            return booster.predict(dmat)
