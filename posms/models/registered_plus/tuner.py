# posms/model/registered_plus/tuner.py
from __future__ import annotations
from typing import Dict


def get_best_params() -> Dict:
    """書留＋レターパックプラスモデル用のベストパラメータを返す。

    ひとまず固定値。あとで Optuna からの結果に差し替えてOK。
    """
    return {
        "max_depth": 6,
        "learning_rate": 0.05,
        "n_estimators": 800,
        "subsample": 0.9,
        "colsample_bytree": 0.9,
        "min_child_weight": 3,
        "gamma": 0.0,
        "reg_alpha": 0.0,
        "reg_lambda": 1.0,
    }
