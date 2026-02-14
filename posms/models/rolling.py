# posms/models/rolling.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as _date
from typing import Protocol, Optional, Tuple

import numpy as np
import pandas as pd

from posms.features.builder import FeatureBuilder, FEATURE_COLUMNS


class PredictorLike(Protocol):
    """ModelPredictor 互換インターフェース（.predict(X)->np.ndarray を想定）"""

    def predict(self, X: pd.DataFrame) -> np.ndarray: ...


@dataclass(frozen=True)
class RollingForecastResult:
    df: pd.DataFrame
    """
    columns:
      - date
      - actual_volume
      - y_filled（ローリング用の系列：実績＋予測）
      - y_pred（予測した日だけ値、それ以外NaN）
      - plus FEATURE_COLUMNS（デバッグ用）
    """
    start: pd.Timestamp
    end: pd.Timestamp
    as_of: pd.Timestamp


def rolling_forecast_28d(
    *,
    fb: FeatureBuilder,
    predictor: PredictorLike,
    start: str | _date,
    days: int = 28,
    context_days: int = 7,
) -> RollingForecastResult:
    """
    止まらないローリング予測（純関数）

    - 実績がある最終日 as_of を検出
    - start が as_of+1 より先なら、as_of+1〜start-1 を内部的にローリング予測して埋める
    - start〜start+days-1 をローリング予測し、結果を DataFrame で返す

    NOTE:
    - DB更新はしない（次工程）
    - 欠損（=未来で実績が無い）は y_filled を予測で埋めるため問題にならない
    """

    start_ts = pd.to_datetime(str(start)).normalize()
    if days <= 0:
        raise ValueError("days must be > 0")
    end_ts = start_ts + pd.Timedelta(days=days - 1)

    # ローリングに必要な前文脈（lag_7, rolling_mean_7 前提）
    seed_start = start_ts - pd.Timedelta(days=context_days)
    # “ギャップ埋め”も含めるため、最低でも as_of+1 から end まで回す
    # as_of は実績の最終日（actual_volume notna の最後）
    # → as_of を取るために、まず実績を読む
    base = fb._load_mail()  # privateだが、今は“骨格づくり”として許容
    base = base.sort_values("date")
    if base["actual_volume"].notna().sum() == 0:
        raise ValueError("actual_volume が1件もありません（ローリングの起点が作れません）。")

    as_of = base.loc[base["actual_volume"].notna(), "date"].max().normalize()

    # 予測を回す開始点（実績が尽きた翌日）
    roll_start = min(seed_start, as_of)  # build_frame の期間下限用（安全）
    roll_begin = as_of + pd.Timedelta(days=1)

    # 予測が必要な最終日（最低でも end_ts まで）
    roll_end = end_ts

    # 未来日も含むフレームを作る（actual_volumeは未来はNaN）
    # ここでは lag元はまだ actual_volume のまま。y_filled を後で作って差し替える。
    df = fb.build_frame(roll_start.date(), roll_end.date(), include_future=True, y_col="actual_volume")

    # ローリング用系列を用意：実績をコピーし、未来は予測で埋める
    df["y_filled"] = df["actual_volume"].astype(float)
    df["y_pred"] = np.nan

    # index を date に揃える（扱いやすさ）
    df = df.sort_values("date").reset_index(drop=True)

    # ローリングを開始する日（実績が尽きていないなら start 以前でもOK）
    # ただし、start が as_of+1 より過去なら「未来予測」自体が不要なので、
    # start〜end のうち actual が無い日だけ予測する。
    # 今回の要件は “足りないときに止まらない” なので、基本は as_of+1 以降だけ回せばよい。
    # ただし、start が as_of より前でも、出力は start〜end の y_pred を作らない（NaNのまま）でよい。
    # → ここでは roll_day を as_of+1 から roll_end まで回す。
    if roll_begin <= roll_end:
        # 予測ループ
        for ts in pd.date_range(roll_begin, roll_end, freq="D"):
            # ts 行の特徴量を作るために、lag元系列を y_filled として再計算する
            # ただし全部毎回計算すると遅いので、必要部分だけ更新…という最適化は後で。
            # いまは正しさ優先。
            df_feat = fb._add_features(df, y_col="y_filled")  # private呼び出し（後で整理）

            row = df_feat.loc[df_feat["date"] == ts]
            if row.empty:
                raise RuntimeError(f"internal error: date row not found: {ts}")

            X_row = row[FEATURE_COLUMNS].astype(float)

            # lag/rolling が NaN の場合は、まだseedが足りない（context_days不足 or 実績ゼロ）
            # ここで止めずに埋めるための保険として、最低限の補完を入れる。
            # デモ前提：NaNは 0 で埋める（次工程で改良可能）
            if X_row.isna().any(axis=None):
                X_row = X_row.fillna(0.0)

            yhat = float(predictor.predict(X_row)[0])

            # y_filled を更新（次の日の lag/rolling が作れる）
            m = df["date"] == ts
            df.loc[m, "y_filled"] = yhat
            df.loc[m, "y_pred"] = yhat

    # 出力範囲を切り出し（start-7〜end を残してデバッグできるようにしておく）
    out_min = seed_start
    out_max = end_ts
    out = df[(df["date"] >= out_min) & (df["date"] <= out_max)].copy()

    # 参考：特徴量も出しておく（デバッグ用）
    out = fb._add_features(out, y_col="y_filled")

    return RollingForecastResult(
        df=out.reset_index(drop=True),
        start=start_ts,
        end=end_ts,
        as_of=as_of,
    )
