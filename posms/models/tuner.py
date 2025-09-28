# posms/models/tuner.py
"""
posms.models.tuner
==================

Optuna で XGBoost のハイパーパラメータを探索するユーティリティ。
- 目的関数: TimeSeriesSplit による CV の RMSE 平均（小さいほど良い）
- 学習は XGBoost 純正 API（DMatrix + xgb.train）を使用し、early_stopping で最良点を拾う
- 戻り値は ModelTrainer(trainer.py) にそのまま渡せる sklearn 互換キーの dict
"""

from __future__ import annotations
from typing import Dict, Optional, Tuple, List

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit

try:
    import optuna
except Exception as e:
    raise RuntimeError("optuna がインストールされていません。`pip install optuna` を実行してください。") from e


def tune_xgb_optuna(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    n_trials: int = 50,
    timeout: Optional[int] = None,   # 秒
    seed: int = 42,
    n_splits: int = 3,
    max_rounds: int = 5000,
    early_stopping_rounds: int = 200,
    use_hist: bool = True,
) -> Tuple[Dict, float]:
    """
    Returns
    -------
    best_params : dict
        sklearn 互換キー（learning_rate, max_depth, subsample, ...）で返す
    best_value : float
        最良試行の CV RMSE（平均）
    """
    X = X.astype(np.float32, copy=False)
    y_arr = np.asarray(y, dtype=np.float32).reshape(-1)

    sampler = optuna.samplers.TPESampler(seed=seed)
    pruner = optuna.pruners.MedianPruner(n_warmup_steps=5)

    def objective(trial: "optuna.Trial") -> float:
        # 探索空間（必要に応じて調整可）
        params = {
            "objective": "reg:squarederror",
            "eta": trial.suggest_float("eta", 0.01, 0.3, log=True),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "gamma": trial.suggest_float("gamma", 0.0, 5.0),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
            "alpha": trial.suggest_float("alpha", 1e-8, 10.0, log=True),
            "lambda": trial.suggest_float("lambda", 1e-8, 10.0, log=True),
            "eval_metric": "rmse",
            "seed": seed,
        }
        if use_hist:
            params["tree_method"] = "hist"

        tscv = TimeSeriesSplit(n_splits=n_splits)
        rmses: List[float] = []

        for tr_idx, va_idx in tscv.split(X):
            dtr = xgb.DMatrix(X.iloc[tr_idx], label=y_arr[tr_idx], missing=np.nan)
            dva = xgb.DMatrix(X.iloc[va_idx], label=y_arr[va_idx], missing=np.nan)

            bst = xgb.train(
                params,
                dtr,
                num_boost_round=max_rounds,
                evals=[(dtr, "train"), (dva, "valid")],
                early_stopping_rounds=early_stopping_rounds,
                verbose_eval=False,
            )

            # best_iteration までで予測
            try:
                pred = bst.predict(dva, iteration_range=(0, bst.best_iteration + 1))
            except TypeError:
                ntree_limit = getattr(bst, "best_ntree_limit", None)
                pred = bst.predict(dva, ntree_limit=ntree_limit) if ntree_limit else bst.predict(dva)

            rmse = float(np.sqrt(np.mean((y_arr[va_idx] - pred) ** 2)))
            rmses.append(rmse)

            # pruner 用に中間値を報告（平均の暫定値）
            trial.report(float(np.mean(rmses)), step=len(rmses))
            if trial.should_prune():
                raise optuna.TrialPruned()

        return float(np.mean(rmses))

    study = optuna.create_study(direction="minimize", sampler=sampler, pruner=pruner)
    study.optimize(objective, n_trials=n_trials, timeout=timeout)

    bp = study.best_params  # "eta", "max_depth", ...
    # ModelTrainer に渡しやすい sklearn 互換キーへ変換
    best_params = {
        "objective": "reg:squarederror",
        "learning_rate": float(bp["eta"]),
        "max_depth": int(bp["max_depth"]),
        "subsample": float(bp["subsample"]),
        "colsample_bytree": float(bp["colsample_bytree"]),
        "gamma": float(bp["gamma"]),
        "min_child_weight": int(bp["min_child_weight"]),
        "alpha": float(bp["alpha"]),
        "lambda": float(bp["lambda"]),
        "eval_metric": "rmse",
        "random_state": seed,
    }
    if use_hist:
        best_params["tree_method"] = "hist"

    return best_params, float(study.best_value)
