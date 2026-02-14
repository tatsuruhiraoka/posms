# posms/models/registered_plus/pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Dict

import pandas as pd

from posms.models.registered_plus.features import (
    TARGET,
    FEATURES_REGISTERED_PLUS,
    build_registered_plus_features,
)
from posms.models.registered_plus.trainer import train as _train
from posms.models.registered_plus.metrics import calc_metrics


# =========================
# Train (from historical)
# =========================


@dataclass(frozen=True)
class TrainResult:
    model: object
    run_id: str
    metrics: dict
    df_train: pd.DataFrame
    df_valid: pd.DataFrame


def _split_time_series(
    df_feat: pd.DataFrame,
    valid_days: int = 60,
    valid_start_date: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    時系列で train/valid を分割する。
    - valid_start_date を指定したら、その日付以降が valid
    - 指定がなければ末尾 valid_days を valid
    """
    if valid_start_date is not None:
        d0 = pd.Timestamp(valid_start_date)
        df_train = df_feat.loc[df_feat.index < d0].copy()
        df_valid = df_feat.loc[df_feat.index >= d0].copy()
    else:
        if valid_days <= 0:
            raise ValueError("valid_days must be positive")
        if len(df_feat) <= valid_days:
            raise ValueError(
                f"not enough rows for split: n={len(df_feat)}, valid_days={valid_days}"
            )
        df_train = df_feat.iloc[:-valid_days].copy()
        df_valid = df_feat.iloc[-valid_days:].copy()

    if len(df_train) == 0 or len(df_valid) == 0:
        raise ValueError(
            f"split resulted in empty train/valid: train={len(df_train)}, valid={len(df_valid)}"
        )

    return df_train, df_valid


def train_from_hist(
    df_hist: pd.DataFrame,
    valid_days: int = 60,
    valid_start_date: Optional[str] = None,
    experiment_name: str = "posms",
    run_name: str = "posms-registered_plus",
    *,
    model_name: str = "posms_registered_plus",
    tags: Optional[Dict[str, str]] = None,
) -> TrainResult:
    """
    df_hist（生データ）から registered_plus モデルを学習するワンショット関数。

    df_hist 必須列:
      - "書留"
      - "レターパックプラス"
      - "sum"（総物数。registered_plus は sum 系特徴量を使うため必須）

    任意列（無ければ 0 扱い）:
      - "holiday"
      - "is_obon"
      - "is_nenmatsu"

    Returns:
      TrainResult(model, run_id, metrics, df_train, df_valid)
    """
    # 1) 特徴量生成（TARGET + FEATURES が揃った DataFrame）
    df_feat = build_registered_plus_features(df_hist)

    # 2) split
    df_train, df_valid = _split_time_series(
        df_feat=df_feat,
        valid_days=valid_days,
        valid_start_date=valid_start_date,
    )

    # 3) 学習（MLflow log は trainer 内）
    #   tags は trainer に渡し、run に刻む（事故防止）
    base_tags: Dict[str, str] = {
        "model_name": model_name,
        "mail_kind": "registered_plus",
        "feature_set": ",".join(map(str, FEATURES_REGISTERED_PLUS)),
    }
    if tags:
        base_tags.update({k: str(v) for k, v in tags.items()})

    model, run_id = _train(
        df_train=df_train,
        df_valid=df_valid,
        experiment_name=experiment_name,
        run_name=run_name,
        model_name=model_name,
        tags=base_tags,
    )

    # 4) valid metrics（返り値としても保持）
    X_valid = df_valid[FEATURES_REGISTERED_PLUS]
    y_valid = df_valid[TARGET]
    y_pred = model.predict(X_valid)
    metrics = calc_metrics(y_valid, y_pred)

    return TrainResult(
        model=model,
        run_id=run_id,
        metrics=metrics,
        df_train=df_train,
        df_valid=df_valid,
    )