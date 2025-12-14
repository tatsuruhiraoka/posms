# posms/model/registered_plus/features.py

from __future__ import annotations
import pandas as pd

TARGET = "registered_plus"

FEATURES_REGISTERED_PLUS = [
    "weekday",
    "holiday",
    "is_obon",
    "is_nenmatsu",
    "sum_lag_1",
    "sum_rm7",
    "sum_lag_7",
]


def build_registered_plus_features(df_hist: pd.DataFrame) -> pd.DataFrame:
    """
    （学習用）過去データから registered_plus 用特徴量を作る。
    """
    df = df_hist.copy()

    df[TARGET] = df["書留"] + df["レターパックプラス"]

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("index が DatetimeIndex である必要があります")

    df["weekday"] = df.index.weekday

    for col in ["holiday", "is_obon", "is_nenmatsu"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].fillna(0).astype(int)

    if "sum" not in df.columns:
        raise ValueError("df_hist に 'sum' 列（総物数）が必要です")

    df["sum_lag_1"] = df["sum"].shift(1)
    df["sum_lag_7"] = df["sum"].shift(7)
    df["sum_rm7"] = df["sum"].shift(1).rolling(7).mean()

    df_feat = df[[TARGET] + FEATURES_REGISTERED_PLUS].dropna()
    return df_feat


def build_registered_plus_future_features(
    df_all: pd.DataFrame,
    pred_start_date: pd.Timestamp,
) -> pd.DataFrame:
    """
    （予測用）過去+未来を含む df_all から、未来日用の特徴量を作る。

    Parameters
    ----------
    df_all : pd.DataFrame
        index: DatetimeIndex（連続した日付）
        columns:
          - 'sum'（過去は実績、未来は通常モデルの予測値）
          - 'holiday', 'is_obon', 'is_nenmatsu'（なければ 0 補完）
    pred_start_date : pd.Timestamp
        「ここから先を registered_plus で予測する」開始日。
        例: pd.Timestamp("2026-01-01")

    Returns
    -------
    df_future_feat : pd.DataFrame
        未来日のみを index に持ち、
        FEATURES_REGISTERED_PLUS をすべて含む DataFrame。
    """
    df = df_all.copy()

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("index が DatetimeIndex である必要があります")

    # 曜日
    df["weekday"] = df.index.weekday

    # フラグ類（無ければ 0）
    for col in ["holiday", "is_obon", "is_nenmatsu"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].fillna(0).astype(int)

    # sum の存在チェック
    if "sum" not in df.columns:
        raise ValueError("df_all に 'sum' 列（総物数 or その予測値）が必要です")

    # lag/rolling は「過去〜未来を含めた全期間」で計算
    df["sum_lag_1"] = df["sum"].shift(1)
    df["sum_lag_7"] = df["sum"].shift(7)
    df["sum_rm7"] = df["sum"].shift(1).rolling(7).mean()

    # 未来日のみ抽出
    mask_future = df.index >= pred_start_date
    df_future = df.loc[mask_future, FEATURES_REGISTERED_PLUS].copy()

    # 未来区間の中でも、計算に足りない先頭行は NaN になるので落とす
    df_future_feat = df_future.dropna()

    return df_future_feat
