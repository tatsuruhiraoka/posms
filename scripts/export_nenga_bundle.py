from __future__ import annotations

import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine

from posms.models.nenga.features import NengaFeatureBuilder

# =========================
# Settings
# =========================
DB_PATH = Path("excel_templates/posms_demo.db").resolve()
OFFICE_ID = 1

OUT_ROOT = Path("model_bundle")
OUT_ASM = OUT_ROOT / "nenga_assembly"
OUT_DEL = OUT_ROOT / "nenga_delivery"

# 組立（GitHubの定義）
FEATURES_ASM = ["year", "nenga_prep_offset", "lag_365", "lag_730", "lag_1095"]

# 配達（GitHubの定義）
FEATURES_JAN1 = ["year", "lag_365"]
FEATURES_JAN3 = ["year", "lag_365"]
FEATURES_AFTER = ["year", "after_newyear_offset", "lag_365"]


def _fit_lr(df: pd.DataFrame, features: list[str]) -> LinearRegression:
    missing = [c for c in features if c not in df.columns]
    if missing:
        raise RuntimeError(f"missing feature columns: {missing}")

    X = df[features].astype(float)
    y = df["actual_volume"].astype(float)

    # 学習データに NaN が混ざると落ちるので除外
    m = X.notna().all(axis=1) & y.notna()
    X = X.loc[m]
    y = y.loc[m]

    if len(X) < 3:
        raise RuntimeError(f"too few training rows after dropna: {len(X)}")

    model = LinearRegression()
    model.fit(X, y)
    return model


def main() -> None:
    if not DB_PATH.exists():
        raise RuntimeError(f"DB not found: {DB_PATH}")

    os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH.as_posix()}"
    eng = create_engine(os.environ["DATABASE_URL"], future=True)

    # -------------------------
    # nenga_assembly
    # -------------------------
    df_asm = NengaFeatureBuilder(eng, office_id=OFFICE_ID, mail_kind="nenga_assembly").build()
    # 念のため日付正規化＆ソート
    df_asm["date"] = pd.to_datetime(df_asm["date"]).dt.normalize()
    df_asm = df_asm.sort_values("date").reset_index(drop=True)

    # 組立フラグ期間だけ使う（それ以外はノイズ）
    if "is_nenga_prep" in df_asm.columns:
        df_asm = df_asm[df_asm["is_nenga_prep"].astype(int).eq(1)].copy()

    m_asm = _fit_lr(df_asm, FEATURES_ASM)

    OUT_ASM.mkdir(parents=True, exist_ok=True)
    joblib.dump(m_asm, OUT_ASM / "model.joblib")

    # -------------------------
    # nenga_delivery
    # -------------------------
    df_del = NengaFeatureBuilder(eng, office_id=OFFICE_ID, mail_kind="nenga_delivery").build()
    df_del["date"] = pd.to_datetime(df_del["date"]).dt.normalize()
    df_del = df_del.sort_values("date").reset_index(drop=True)

    # 配達期間（1/1-1/15）だけ
    if "is_nenga_delivery" in df_del.columns:
        df_del = df_del[df_del["is_nenga_delivery"].astype(int).eq(1)].copy()

    required = ["is_newyear_day", "is_jan3", "is_after_newyear"]
    missing_req = [c for c in required if c not in df_del.columns]
    if missing_req:
        raise RuntimeError(f"nenga_delivery missing required flags: {missing_req}")

    df_jan1 = df_del[df_del["is_newyear_day"].astype(int).eq(1)].copy()
    df_jan3 = df_del[df_del["is_jan3"].astype(int).eq(1)].copy()
    df_after = df_del[df_del["is_after_newyear"].astype(int).eq(1)].copy()

    m_jan1 = _fit_lr(df_jan1, FEATURES_JAN1)
    m_jan3 = _fit_lr(df_jan3, FEATURES_JAN3)
    m_after = _fit_lr(df_after, FEATURES_AFTER)

    OUT_DEL.mkdir(parents=True, exist_ok=True)
    joblib.dump(m_jan1, OUT_DEL / "model_jan1.joblib")
    joblib.dump(m_jan3, OUT_DEL / "model_jan3.joblib")
    joblib.dump(m_after, OUT_DEL / "model_after.joblib")

    print("OK: exported nenga bundles:")
    print(f" - {OUT_ASM / 'model.joblib'}")
    print(f" - {OUT_DEL / 'model_jan1.joblib'}")
    print(f" - {OUT_DEL / 'model_jan3.joblib'}")
    print(f" - {OUT_DEL / 'model_after.joblib'}")


if __name__ == "__main__":
    main()