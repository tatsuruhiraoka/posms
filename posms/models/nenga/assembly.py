# posms/models/nenga/assembly.py
from __future__ import annotations

import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from posms.models.nenga.features import NengaFeatureBuilder

FEATURES_NENGA_ASSEMBLY = [
    "year",
    "nenga_prep_offset",
    "lag_365",
    "lag_730",
    "lag_1095",
]


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


def _load_model() -> object:
    p = _bundle_root() / "nenga_assembly" / "model.joblib"
    if not p.exists():
        raise RuntimeError(f"nenga_assembly model not found: {p}")
    return joblib.load(p)


def predict(
    engine,
    *,
    office_id: int,
    round_to_1000: bool = True,
) -> np.ndarray:
    """
    年賀組立 (nenga_assembly) を model_bundle から予測して返す（DB更新なし）。
    欠損行（lag等がNaN）は予測せず 0 とする（影響範囲を最小化）。
    """
    df = NengaFeatureBuilder(engine, office_id=office_id, mail_kind="nenga_assembly").build()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date").reset_index(drop=True)

    missing = [c for c in FEATURES_NENGA_ASSEMBLY if c not in df.columns]
    if missing:
        raise RuntimeError(f"nenga_assembly: missing feature columns: {missing}")

    X = df[FEATURES_NENGA_ASSEMBLY].astype(float)
    mask_ok = X.notna().all(axis=1)

    model = _load_model()

    y = np.zeros(len(df), dtype=float)
    if mask_ok.any():
        y[mask_ok.values] = np.asarray(model.predict(X.loc[mask_ok]), dtype=float)

    y = np.clip(y, 0.0, None)

    if round_to_1000:
        y = np.round(y / 1000.0) * 1000.0

    return y.astype(int)