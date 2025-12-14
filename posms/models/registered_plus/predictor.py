# posms/model/registered_plus/predictor.py
from __future__ import annotations

from typing import Sequence

import pandas as pd

from posms.models.registered_plus.features import FEATURES_REGISTERED_PLUS


def predict(model, df_future: pd.DataFrame) -> pd.Series:
    """将来データに対する registered_plus 予測を返す。

    df_future は FEATURES_REGISTERED_PLUS をすべて持っている前提。
    """
    X = df_future[FEATURES_REGISTERED_PLUS].copy()
    y_pred = model.predict(X)
    return pd.Series(y_pred, index=df_future.index, name="registered_plus_pred")
