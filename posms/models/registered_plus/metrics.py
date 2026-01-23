# posms/model/registered_plus/metrics.py
from __future__ import annotations

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error


def calc_metrics(y_true, y_pred) -> dict:
    """検証データに対する評価指標をまとめて返す."""
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))

    return {
        "mae": float(mae),
        "rmse": float(rmse),
    }
