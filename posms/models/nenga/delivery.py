# posms/models/nenga/delivery.py
from __future__ import annotations

import numpy as np
import pandas as pd
import mlflow
from sklearn.linear_model import LinearRegression

from posms.models.nenga.features import NengaFeatureBuilder

MAIL_KIND = "nenga_delivery"
TARGET_COL = "actual_volume"

# ---- Linear feature sets ----
FEATURES_JAN1 = ["year", "lag_365"]  # 1/1
FEATURES_JAN3 = ["year", "lag_365"]  # 1/3
FEATURES_AFTER = ["year", "after_newyear_offset", "lag_365"]  # 1/4〜1/15（配達日のみ）


def _round_to_thousands(x: float) -> int:
    """千単位で四捨五入（必要なら切り捨て/切り上げに変更）"""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0
    return int(round(float(x) / 1000.0) * 1000)


def _ensure_date_series(df: pd.DataFrame) -> pd.Series:
    if "date" in df.columns:
        return pd.to_datetime(df["date"])
    if isinstance(df.index, pd.DatetimeIndex):
        return pd.to_datetime(df.index)
    raise ValueError("df に 'date' 列または DatetimeIndex が必要です")


def _holiday_array(df: pd.DataFrame) -> np.ndarray:
    if "holiday" in df.columns:
        return (
            pd.to_numeric(df["holiday"], errors="coerce").fillna(0).astype(int).values
        )
    return np.zeros(len(df), dtype=int)


def _build_deliver_mask(df: pd.DataFrame) -> pd.Series:
    """
    配達日判定（あなたの決定ルール）:
      - 1/1 と 1/3 は土日祝でも配達
      - 1/2 は休み（配達しない）
      - 1/4以降は土日祝休み（holiday==1 は休み）
    """
    d = _ensure_date_series(df)
    hol = _holiday_array(df)

    is_jan = d.dt.month == 1
    day = d.dt.day

    deliver = is_jan & day.isin([1, 3])  # 1/1, 1/3 は必ず配達
    deliver = deliver | (
        is_jan & (day >= 4) & (hol == 0)
    )  # 1/4以降：holiday==0 の日だけ配達
    deliver = deliver | (~is_jan & (hol == 0))  # 念のため（期間外）

    deliver = deliver & ~(is_jan & (day == 2))  # 1/2 は必ず休み
    return deliver.astype(bool)


def _apply_carry(df: pd.DataFrame, preds: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    休み日の予測分を次の配達日に繰り越す。
    返り値: (carry適用後out, deliver_mask)
    """
    out = preds.astype(float).copy()
    out = out.fillna(0.0).clip(lower=0.0)

    d = _ensure_date_series(df)
    deliver = _build_deliver_mask(df)
    is_jan2 = (d.dt.month == 1) & (d.dt.day == 2)

    carry = 0.0
    last_delivery_pos: int | None = None

    for pos in range(len(df)):
        if bool(is_jan2.iloc[pos]):
            carry += float(out.iloc[pos])
            out.iloc[pos] = 0.0
            continue

        if bool(deliver.iloc[pos]):
            out.iloc[pos] = float(out.iloc[pos]) + carry
            carry = 0.0
            last_delivery_pos = pos
        else:
            carry += float(out.iloc[pos])
            out.iloc[pos] = 0.0

    if carry > 0:
        if last_delivery_pos is not None:
            out.iloc[last_delivery_pos] = float(out.iloc[last_delivery_pos]) + carry
        else:
            out.iloc[-1] = float(out.iloc[-1]) + carry

    return out.clip(lower=0.0), deliver


def _round_blockwise_preserve_total(out: pd.Series, deliver: pd.Series) -> pd.Series:
    """
    配達日のみ千単位に丸めつつ、丸め誤差をブロック終端に吸収して
    ブロック内の総量が極端に痩せないようにする。

    ブロック = 連続する配達日（休みで区切る）
    """
    out2 = out.astype(float).copy()
    out2 = out2.fillna(0.0).clip(lower=0.0)

    deliver_arr = deliver.astype(bool).values
    n = len(out2)
    i = 0

    while i < n:
        if not deliver_arr[i]:
            i += 1
            continue

        j = i
        while j < n and deliver_arr[j]:
            j += 1

        block = out2.iloc[i:j].values.astype(float)
        total = float(block.sum())

        rounded = np.array([_round_to_thousands(x) for x in block], dtype=float)
        diff = total - float(rounded.sum())
        adj = _round_to_thousands(diff)
        rounded[-1] = max(0.0, float(rounded[-1] + adj))

        out2.iloc[i:j] = rounded
        i = j

    return out2.clip(lower=0.0)


def _postprocess(df: pd.DataFrame, preds: pd.Series) -> pd.Series:
    """
    運用後処理：
      1) 休み日の予測分を繰り越し
      2) 千単位丸め（ブロック誤差吸収）
    """
    out, deliver = _apply_carry(df, preds)
    out = _round_blockwise_preserve_total(out, deliver)
    return out


def _fit_linear(df: pd.DataFrame, feat_cols: list[str]) -> LinearRegression:
    model = LinearRegression()
    X = df[feat_cols].astype(float)
    y = df[TARGET_COL].astype(float)
    model.fit(X, y)
    return model


def train(engine, *, office_id: int, experiment: str = "posms_nenga_delivery") -> str:
    """
    年賀配達（全部線形）を学習して MLflow に保存。run_id を返す。

    モデル：
      - 1/1: LinearRegression (FEATURES_JAN1)
      - 1/3: LinearRegression (FEATURES_JAN3)
      - 1/4〜1/15（配達日）: LinearRegression (FEATURES_AFTER)
    """
    fb = NengaFeatureBuilder(engine, office_id=office_id, mail_kind=MAIL_KIND)
    df = fb.build().copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 学習は actual がある行のみ
    df_act = df[df[TARGET_COL].notna()].copy()

    # 1/1
    df_jan1 = df_act[
        (df_act["date"].dt.month == 1) & (df_act["date"].dt.day == 1)
    ].dropna(subset=FEATURES_JAN1 + [TARGET_COL])
    model_jan1 = (
        _fit_linear(df_jan1, FEATURES_JAN1) if len(df_jan1) > 0 else LinearRegression()
    )

    # 1/3
    df_jan3 = df_act[
        (df_act["date"].dt.month == 1) & (df_act["date"].dt.day == 3)
    ].dropna(subset=FEATURES_JAN3 + [TARGET_COL])
    model_jan3 = (
        _fit_linear(df_jan3, FEATURES_JAN3) if len(df_jan3) > 0 else LinearRegression()
    )

    # 1/4〜1/15（配達日だけ学習に使う）
    deliver_mask = _build_deliver_mask(df_act)
    df_after = df_act[
        (df_act["date"].dt.month == 1)
        & (df_act["date"].dt.day >= 4)
        & (df_act["date"].dt.day <= 15)
        & (deliver_mask.values)
    ].dropna(subset=FEATURES_AFTER + [TARGET_COL])

    model_after = (
        _fit_linear(df_after, FEATURES_AFTER)
        if len(df_after) > 0
        else LinearRegression()
    )

    mlflow.set_experiment(experiment)
    with mlflow.start_run() as run:
        mlflow.log_param("mail_kind", MAIL_KIND)
        mlflow.log_param("office_id", int(office_id))
        mlflow.log_param("model_jan1", "LinearRegression")
        mlflow.log_param("model_jan3", "LinearRegression")
        mlflow.log_param("model_after", "LinearRegression")
        mlflow.log_param("features_jan1", ",".join(FEATURES_JAN1))
        mlflow.log_param("features_jan3", ",".join(FEATURES_JAN3))
        mlflow.log_param("features_after", ",".join(FEATURES_AFTER))
        mlflow.log_param("train_rows_jan1", int(len(df_jan1)))
        mlflow.log_param("train_rows_jan3", int(len(df_jan3)))
        mlflow.log_param("train_rows_after", int(len(df_after)))

        mlflow.sklearn.log_model(model_jan1, artifact_path="model_jan1")
        mlflow.sklearn.log_model(model_jan3, artifact_path="model_jan3")
        mlflow.sklearn.log_model(model_after, artifact_path="model_after")

        return run.info.run_id


def predict(engine, *, office_id: int, run_id: str) -> pd.Series:
    """
    学習済み（全部線形）モデルで予測し、運用ルール（繰り越し+丸め）を適用。
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

    model_jan1 = mlflow.sklearn.load_model(f"runs:/{run_id}/model_jan1")
    model_jan3 = mlflow.sklearn.load_model(f"runs:/{run_id}/model_jan3")
    model_after = mlflow.sklearn.load_model(f"runs:/{run_id}/model_after")

    preds = pd.Series(np.nan, index=df.index, dtype=float)

    # 1/1
    m1 = (
        (df["date"].dt.month == 1)
        & (df["date"].dt.day == 1)
        & df[FEATURES_JAN1].notna().all(axis=1)
    )
    if m1.any():
        preds.loc[m1] = model_jan1.predict(df.loc[m1, FEATURES_JAN1].astype(float))

    # 1/3
    m3 = (
        (df["date"].dt.month == 1)
        & (df["date"].dt.day == 3)
        & df[FEATURES_JAN3].notna().all(axis=1)
    )
    if m3.any():
        preds.loc[m3] = model_jan3.predict(df.loc[m3, FEATURES_JAN3].astype(float))

    # 1/2〜1/15（配達日・休み日含む予測値は後でcarryへ）
    ma = (
        (df["date"].dt.month == 1)
        & (df["date"].dt.day >= 2)
        & (df["date"].dt.day <= 15)
        & df[FEATURES_AFTER].notna().all(axis=1)
    )
    if ma.any():
        preds.loc[ma] = model_after.predict(df.loc[ma, FEATURES_AFTER].astype(float))

    preds = preds.fillna(0.0).clip(lower=0.0)

    # 運用後処理（繰り越し + 丸め）
    preds = _postprocess(df, preds)

    return preds
