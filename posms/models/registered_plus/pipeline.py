# posms/model/registered_plus/pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd

from posms.models.registered_plus.features import (
    TARGET,
    FEATURES_REGISTERED_PLUS,
    build_registered_plus_features,
    build_registered_plus_future_features,
)
from posms.models.registered_plus.trainer import train as _train
from posms.models.registered_plus.metrics import calc_metrics
from posms.models.registered_plus.predictor import predict as _predict


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
    model, run_id = _train(
        df_train=df_train,
        df_valid=df_valid,
        experiment_name=experiment_name,
        run_name=run_name,
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


# =========================
# Forecast (chained from sum)
# =========================


@dataclass(frozen=True)
class ForecastResult:
    y_pred: pd.Series  # registered_plus 予測（index=日付）
    df_future_feat: pd.DataFrame  # 予測に使った未来特徴量（確認用）
    df_all: pd.DataFrame  # 連鎖に使った過去+未来（確認用）


def forecast_from_sum(
    model_registered_plus,
    df_hist: pd.DataFrame,
    sum_future: pd.Series,
    pred_start_date: str | pd.Timestamp,
    holiday_future: Optional[pd.Series] = None,
    is_obon_future: Optional[pd.Series] = None,
    is_nenmatsu_future: Optional[pd.Series] = None,
) -> ForecastResult:
    """
    normal モデル等で作った sum（総物数）の未来予測を使って、
    registered_plus を連鎖予測する。

    Parameters
    ----------
    model_registered_plus:
        学習済み registered_plus モデル（XGBRegressor 等）
    df_hist:
        過去の実績データ（index=日付 DatetimeIndex）
        必須: 'sum' 列（過去実績）
        任意: holiday/is_obon/is_nenmatsu（なければ 0 扱い）
    sum_future:
        未来の sum 予測（index=日付 DatetimeIndex、値=float）
        ※ df_hist.index の翌日以降など未来区間を含む想定
    pred_start_date:
        registered_plus を予測したい開始日（この日以降が予測対象）
    holiday_future, is_obon_future, is_nenmatsu_future:
        未来日のフラグ Series（index=日付）。無ければ 0 扱い。

    Returns
    -------
    ForecastResult
        y_pred: registered_plus 予測
        df_future_feat: 予測に使った未来特徴量
        df_all: 過去+未来の結合データ
    """
    pred_start_date = pd.Timestamp(pred_start_date)

    df_hist2 = df_hist.copy()
    if "sum" not in df_hist2.columns:
        raise ValueError("df_hist に 'sum' 列（過去実績）が必要です")
    if not isinstance(df_hist2.index, pd.DatetimeIndex):
        raise ValueError("df_hist.index は DatetimeIndex である必要があります")
    if not isinstance(sum_future.index, pd.DatetimeIndex):
        raise ValueError("sum_future.index は DatetimeIndex である必要があります")

    # 未来側 DataFrame を作る
    df_future = pd.DataFrame(index=sum_future.index)
    df_future["sum"] = sum_future.astype(float)

    def _set_future_flag(name: str, s: Optional[pd.Series]):
        if s is None:
            df_future[name] = 0
        else:
            df_future[name] = s.reindex(df_future.index).fillna(0).astype(int)

    _set_future_flag("holiday", holiday_future)
    _set_future_flag("is_obon", is_obon_future)
    _set_future_flag("is_nenmatsu", is_nenmatsu_future)

    # 過去側にもフラグ列が無ければ作る（0）
    for col in ["holiday", "is_obon", "is_nenmatsu"]:
        if col not in df_hist2.columns:
            df_hist2[col] = 0
        df_hist2[col] = df_hist2[col].fillna(0).astype(int)

    # 過去+未来を結合（重複日付があれば未来を優先）
    df_all = pd.concat(
        [
            df_hist2[["sum", "holiday", "is_obon", "is_nenmatsu"]],
            df_future[["sum", "holiday", "is_obon", "is_nenmatsu"]],
        ],
        axis=0,
    )
    df_all = df_all[~df_all.index.duplicated(keep="last")].sort_index()

    # registered_plus 未来特徴量を作る（sum lag/rolling は df_all 全体で計算）
    df_future_feat = build_registered_plus_future_features(
        df_all=df_all,
        pred_start_date=pred_start_date,
    )

    # 予測
    y_pred = _predict(model_registered_plus, df_future_feat)

    return ForecastResult(
        y_pred=y_pred,
        df_future_feat=df_future_feat,
        df_all=df_all,
    )
