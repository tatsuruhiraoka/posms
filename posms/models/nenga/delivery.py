# posms/models/nenga/delivery.py
from __future__ import annotations

import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from posms.models.nenga.features import NengaFeatureBuilder

FEATURES_JAN1 = ["year", "lag_365"]
FEATURES_JAN3 = ["year", "lag_365"]
FEATURES_AFTER = ["year", "after_newyear_offset", "lag_365"]


def _bundle_root() -> Path:
    p = os.getenv("POSMS_BUNDLE_DIR")
    if p:
        return Path(p).expanduser().resolve()

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).parent
        for c in (exe_dir / "_internal" / "model_bundle", exe_dir / "model_bundle"):
            if c.exists():
                return c
        return exe_dir / "_internal" / "model_bundle"

    return (Path.cwd() / "model_bundle").resolve()


def _load_models() -> tuple[object, object, object]:
    base = _bundle_root() / "nenga_delivery"
    p1 = base / "model_jan1.joblib"
    p3 = base / "model_jan3.joblib"
    pa = base / "model_after.joblib"
    missing = [str(p) for p in (p1, p3, pa) if not p.exists()]
    if missing:
        raise RuntimeError(f"nenga_delivery models not found: {missing}")
    return joblib.load(p1), joblib.load(p3), joblib.load(pa)


def _predict_masked(model: object, X: pd.DataFrame) -> np.ndarray:
    """
    欠損行は予測せず0。
    """
    if X.empty:
        return np.zeros(0, dtype=float)
    mask_ok = X.notna().all(axis=1)
    y = np.zeros(len(X), dtype=float)
    if mask_ok.any():
        y[mask_ok.values] = np.asarray(model.predict(X.loc[mask_ok]), dtype=float)
    return y


def predict(
    engine,
    *,
    office_id: int,
) -> np.ndarray:
    """
    年賀配達 (nenga_delivery) を model_bundle から予測して返す（DB更新なし）。
    線形モデル（joblib）3本を使用し、欠損行は0。
    """
    df = NengaFeatureBuilder(engine, office_id=office_id, mail_kind="nenga_delivery").build().copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date").reset_index(drop=True)

    required_cols = [
        "is_newyear_day",
        "is_jan3",
        "is_after_newyear",
        "after_newyear_offset",
        "year",
        "lag_365",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"nenga_delivery: missing required columns: {missing}")

    m_jan1, m_jan3, m_after = _load_models()

    y = np.zeros(len(df), dtype=float)

    mask_jan1 = df["is_newyear_day"].astype(int).eq(1)
    mask_jan3 = df["is_jan3"].astype(int).eq(1)
    mask_after = df["is_after_newyear"].astype(int).eq(1)

    if mask_jan1.any():
        X1 = df.loc[mask_jan1, FEATURES_JAN1].astype(float)
        y[mask_jan1.values] = _predict_masked(m_jan1, X1)

    if mask_jan3.any():
        X3 = df.loc[mask_jan3, FEATURES_JAN3].astype(float)
        y[mask_jan3.values] = _predict_masked(m_jan3, X3)

    if mask_after.any():
        Xa = df.loc[mask_after, FEATURES_AFTER].astype(float)
        y[mask_after.values] = _predict_masked(m_after, Xa)

    y = np.clip(y, 0.0, None)
    return np.round(y).astype(int)