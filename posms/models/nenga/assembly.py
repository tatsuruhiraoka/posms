# posms/models/nenga/assembly.py
from __future__ import annotations

import numpy as np
import pandas as pd
import mlflow
from sklearn.linear_model import LinearRegression

from posms.models.nenga.features import NengaFeatureBuilder

MAIL_KIND = "nenga_assembly"
TARGET_COL = "actual_volume"

FEATURES = [
    "year",
    "nenga_prep_offset",
    "lag_365",
    "lag_730",
    "lag_1095",
]


def _round_to_thousands(x: float) -> int:
    """千単位で四捨五入（必要なら切り捨て/切り上げに変更）"""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0
    return int(round(float(x) / 1000.0) * 1000)


def train(engine, *, office_id: int, experiment: str = "posms_nenga_assembly") -> str:
    """
    年賀組立（線形）を学習して MLflow に保存。run_id を返す。
    """
    fb = NengaFeatureBuilder(engine, office_id=office_id, mail_kind=MAIL_KIND)
    df = fb.build().copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    df_train = df[df[TARGET_COL].notna()].copy()
    df_train = df_train[df_train[TARGET_COL] > 0]  # ←追加（休み=0を学習から除外）
    df_train = df_train.dropna(subset=FEATURES + [TARGET_COL])

    model = LinearRegression()
    if len(df_train) > 0:
        X = df_train[FEATURES].astype(float)
        y = df_train[TARGET_COL].astype(float)
        model.fit(X, y)

    mlflow.set_experiment(experiment)
    with mlflow.start_run() as run:
        mlflow.log_param("mail_kind", MAIL_KIND)
        mlflow.log_param("office_id", int(office_id))
        mlflow.log_param("model", "LinearRegression")
        mlflow.log_param("features", ",".join(FEATURES))
        mlflow.log_param("train_rows", int(len(df_train)))

        mlflow.sklearn.log_model(model, artifact_path="model")

        return run.info.run_id


def predict(
    engine, *, office_id: int, run_id: str, round_to_1000: bool = True
) -> pd.Series:
    """
    学習済み 年賀組立（線形）モデルで raw 予測を返す。
    - 稼働日（組立する/しない）や繰り越しは “シフト作成者の入力” に基づいて PuLP直前で適用する想定
    - ここでは日別の raw 需要を返すだけ

    返り値は df（特徴量）の行インデックスに対応する Series。
    """
    fb = NengaFeatureBuilder(engine, office_id=office_id, mail_kind=MAIL_KIND)
    df = fb.build().copy()

    if "date" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.copy()
            df["date"] = df.index
        else:
            raise ValueError("NengaFeatureBuilder.build() の戻りに 'date' 列が必要です")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    model = mlflow.sklearn.load_model(f"runs:/{run_id}/model")

    preds = pd.Series(0.0, index=df.index, dtype=float)

    mask = df[FEATURES].notna().all(axis=1)
    if mask.any():
        preds.loc[mask] = model.predict(df.loc[mask, FEATURES].astype(float))

    preds = preds.clip(lower=0.0)

    # 丸めは「表示単位」の問題なので、モデル側では任意にする
    if round_to_1000:
        preds = preds.apply(_round_to_thousands).astype(float)

    return preds
