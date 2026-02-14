# posms/models/registered_plus/features.py
from __future__ import annotations

import pandas as pd
import jpholiday

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

# 旧/新の列名ゆらぎをここで吸収（features.py が唯一の入口）
_COMPONENT_COL_CANDIDATES = [
    ("registered", "lp_plus"),             # 新名
    ("書留", "レターパックプラス"),        # 旧名（日本語）
]


def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("index が DatetimeIndex である必要があります")
    # normalize して日次として扱う
    df = df.copy()
    df.index = pd.to_datetime(df.index).normalize()
    return df


def _compute_flags(idx: pd.DatetimeIndex) -> pd.DataFrame:
    """registered_plus 用のフラグを index から計算（唯一の定義）。"""
    s_holiday = idx.map(lambda ts: int(jpholiday.is_holiday(ts.date())))
    s_obon = idx.map(lambda ts: int(ts.month == 8 and 13 <= ts.day <= 16))
    s_nenmatsu = idx.map(
        lambda ts: int((ts.month == 12 and ts.day >= 20) or (ts.month == 1 and ts.day <= 7))
    )
    return pd.DataFrame(
        {
            "holiday": s_holiday.astype(int),
            "is_obon": s_obon.astype(int),
            "is_nenmatsu": s_nenmatsu.astype(int),
        },
        index=idx,
    )


def _resolve_components(df: pd.DataFrame) -> tuple[str, str] | None:
    """df に含まれる registered_plus 構成列（2本）を特定する。"""
    cols = set(df.columns)
    for a, b in _COMPONENT_COL_CANDIDATES:
        if a in cols and b in cols:
            return a, b
    return None


def build_registered_plus_features(df_hist: pd.DataFrame) -> pd.DataFrame:
    """
    （学習用）過去データから registered_plus 用特徴量を作る。

    入力 df_hist：
    - index: DatetimeIndex
    - columns:
        - 2本の構成列（registered/lp_plus または 書留/レターパックプラス）
          もしくは sum がある
        - holiday/is_obon/is_nenmatsu はあってもなくてもよい（無ければここで計算）
    """
    df = _ensure_datetime_index(df_hist)

    # weekday（唯一の定義）
    df["weekday"] = df.index.weekday.astype(int)

    # フラグ（無ければ計算、あれば上書きせず正規化）
    flags = _compute_flags(df.index)
    for c in ["holiday", "is_obon", "is_nenmatsu"]:
        if c not in df.columns:
            df[c] = flags[c]
        df[c] = df[c].fillna(flags[c]).astype(int)

    # sum の用意（無ければ構成列から作る）
    if "sum" not in df.columns:
        comp = _resolve_components(df)
        if comp is None:
            raise ValueError("df_hist に 'sum' が無く、構成列（registered/lp_plus or 書留/レターパックプラス）も見つかりません")
        a, b = comp
        df["sum"] = df[a].fillna(0).astype(float) + df[b].fillna(0).astype(float)
    else:
        df["sum"] = df["sum"].fillna(0).astype(float)

    # TARGET（学習目的変数）
    df[TARGET] = df["sum"].astype(float)

    # lag/rolling（唯一の定義）
    df["sum_lag_1"] = df["sum"].shift(1)
    df["sum_lag_7"] = df["sum"].shift(7)
    df["sum_rm7"] = df["sum"].shift(1).rolling(7).mean()

    df_feat = df[[TARGET] + FEATURES_REGISTERED_PLUS].dropna()
    return df_feat


def build_registered_plus_feature_row(dt: pd.Timestamp, sum_series: pd.Series) -> dict:
    """
    （CLIの自己回帰ローリング用）
    1日分の registered_plus 特徴量を作る。

    sum_series:
      index=日付（DatetimeIndex, 日次）, value=sum（実績＋予測で埋めた系列）
    """
    dt = pd.to_datetime(dt).normalize()

    # series 側も正規化
    s = sum_series.copy()
    if not isinstance(s.index, pd.DatetimeIndex):
        s.index = pd.to_datetime(s.index)
    s.index = pd.to_datetime(s.index).normalize()

    lag_1 = float(s.get(dt - pd.Timedelta(days=1), 0.0))
    lag_7 = float(s.get(dt - pd.Timedelta(days=7), 0.0))

    last7 = s.loc[dt - pd.Timedelta(days=7): dt - pd.Timedelta(days=1)]
    rm7 = float(last7.mean()) if len(last7) > 0 else 0.0

    # フラグは _compute_flags を唯一の定義にする
    flags = _compute_flags(pd.DatetimeIndex([dt])).iloc[0]

    return {
        "weekday": float(dt.weekday()),
        "holiday": float(flags["holiday"]),
        "is_obon": float(flags["is_obon"]),
        "is_nenmatsu": float(flags["is_nenmatsu"]),
        "sum_lag_1": float(lag_1),
        "sum_rm7": float(rm7),
        "sum_lag_7": float(lag_7),
    }

