# posms/forecast_entry.py
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


# -------------------------
# inputs.json
# -------------------------
def _load_inputs(csvdir: Path) -> dict:
    p = csvdir / "inputs.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))

    # フォールバック: shift_meta.csv から start_date を読む
    meta = csvdir / "shift_meta.csv"
    if meta.exists():
        df = pd.read_csv(meta)
        if "start_date" in df.columns and len(df) >= 1:
            start = str(df.loc[0, "start_date"]).strip()
            # days は Excel テンプレが 28 日前提なので固定でOK
            return {"start": start, "days": 28, "office_id": 1}

    raise RuntimeError(
        f"inputs.json not found: {p} (and shift_meta.csv fallback failed)"
    )


# -------------------------
# SQLite DATABASE_URL auto setup (same concept as CLI)
# -------------------------
def _set_sqlite_database_url_if_missing(sqlite_path: str | None) -> None:
    if os.getenv("DATABASE_URL"):
        return

    if sqlite_path:
        db_path = Path(sqlite_path).expanduser().resolve()
    else:
        # exe: sys.executable の隣 / 開発: cwd
        base = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path.cwd()
        # まず POSMS_DB_PATH があれば優先
        p = os.getenv("POSMS_DB_PATH")
        if p:
            db_path = Path(p).expanduser().resolve()
        else:
            db_path = (base / "posms.db").resolve()

    if not db_path.exists():
        raise RuntimeError(
            "DB connection info is incomplete.\n"
            f"SQLite file not found: {db_path}\n"
            "Set DATABASE_URL or POSMS_DB_PATH or pass --sqlite."
        )

    os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"


# -------------------------
# nenga window rule
# -------------------------
def _overlaps(a_start: date, a_end: date, b_start: date, b_end: date) -> bool:
    return not (a_end < b_start or b_end < a_start)


def _should_use_nenga(start: date, days: int) -> bool:
    win_start = start
    win_end = start + timedelta(days=days - 1)

    y = start.year
    seasons = [
        (date(y, 12, 25), date(y + 1, 1, 15)),
        (date(y - 1, 12, 25), date(y, 1, 15)),
    ]
    return any(_overlaps(win_start, win_end, s, e) for s, e in seasons)


def _choose_mail_kinds(start: date, days: int) -> list[str]:
    kinds = ["normal", "registered_plus"]
    if _should_use_nenga(start, days):
        kinds += ["nenga_assembly", "nenga_delivery"]
    return kinds


# -------------------------
# Forecast per kind (NO DB update, output forecast.csv)
# -------------------------
def _forecast_normal(*, office_id: int, start_ts: pd.Timestamp, days: int, eng) -> pd.DataFrame:
    from posms.features.builder import FeatureBuilder
    from posms.models.predictor import ModelPredictor
    from posms.models.rolling import rolling_forecast_28d

    end_ts = start_ts + pd.Timedelta(days=days - 1)

    fb = FeatureBuilder(office_id=office_id, mail_kind="normal", engine=eng)

    pred = ModelPredictor(
        model_name="posms_normal",
        mail_kind="normal",
        office_id=int(office_id),
        experiment=os.getenv("MLFLOW_EXPERIMENT_NAME", "posms"),
        tracking_uri=os.getenv("MLFLOW_TRACKING_URI"),
        stage=None,
    )

    res = rolling_forecast_28d(
        fb=fb,
        predictor=pred,
        start=start_ts.date(),
        days=days,
        context_days=7,
    )

    df_out = res.df.copy()
    df_out["date"] = pd.to_datetime(df_out["date"]).dt.normalize()
    df_out = df_out[(df_out["date"] >= start_ts) & (df_out["date"] <= end_ts)].copy()

    if df_out.empty:
        raise RuntimeError("normal: rolling output is empty")

    if "y_pred" not in df_out.columns or "y_filled" not in df_out.columns:
        raise RuntimeError(f"normal: rolling result missing y_pred/y_filled. cols={list(df_out.columns)}")

    raw = df_out.set_index("date")["y_pred"].astype(float)
    raw = raw.fillna(df_out.set_index("date")["y_filled"].astype(float))
    raw = raw.asfreq("D")

    df_deliver = ModelPredictor.apply_delivery_rules(
        raw,
        round_to_thousand=True,
        extend_to_next_delivery=True,
    )

    df_deliver["date"] = pd.to_datetime(df_deliver["date"]).dt.normalize()
    df_deliver = df_deliver[(df_deliver["date"] >= start_ts) & (df_deliver["date"] <= end_ts)].copy()

    out = df_deliver[["date", "deliver_pred"]].rename(columns={"deliver_pred": "forecast_volume"}).copy()
    out["mail_kind"] = "normal"
    out["forecast_volume"] = out["forecast_volume"].astype(float).round().astype(int)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["date", "mail_kind", "forecast_volume"]]


def _forecast_registered_plus(*, office_id: int, start_ts: pd.Timestamp, days: int, eng) -> pd.DataFrame:
    from sqlalchemy import text
    from posms.models.registered_plus.features import (
        FEATURES_REGISTERED_PLUS,
        build_registered_plus_feature_row,
    )
    from posms.models.predictor import ModelPredictor

    end_ts = start_ts + pd.Timedelta(days=days - 1)

    pred = ModelPredictor(
        model_name="posms_registered_plus",
        mail_kind="registered_plus",
        office_id=int(office_id),
        experiment=os.getenv("MLFLOW_EXPERIMENT_NAME", "posms"),
        tracking_uri=os.getenv("MLFLOW_TRACKING_URI"),
        stage=None,
    )

    def _load_kind(kind: str) -> pd.Series:
        sql = """
            SELECT "date", actual_volume
              FROM mailvolume_by_type
             WHERE office_id=:o AND mail_kind=:k
             ORDER BY "date"
        """
        df = pd.read_sql(text(sql), eng, params={"o": office_id, "k": kind}, parse_dates=["date"])
        if df.empty:
            raise RuntimeError(f"registered_plus: no rows for mail_kind={kind!r}")
        s = df.set_index("date")["actual_volume"].astype(float)
        s.index = pd.to_datetime(s.index).normalize()
        return s

    s_reg = _load_kind("registered")
    s_lp = _load_kind("lp_plus")

    vol = s_reg.add(s_lp, fill_value=0.0).sort_index()

    last_actual_date = vol.dropna().index.max()
    if last_actual_date is None:
        raise RuntimeError("registered_plus: base series is empty")

    bridge_start = pd.to_datetime(last_actual_date).normalize() + pd.Timedelta(days=1)
    start_for_pred = bridge_start if start_ts > bridge_start else start_ts
    full_dates = pd.date_range(start_for_pred, end_ts, freq="D")

    full_idx = pd.date_range(vol.index.min(), end_ts, freq="D")
    vol = vol.reindex(full_idx).fillna(0.0)

    updates: dict[pd.Timestamp, float] = {}

    for dt in full_dates:
        feat_dict = build_registered_plus_feature_row(dt, vol)
        X = pd.DataFrame([feat_dict])[list(FEATURES_REGISTERED_PLUS)].astype(float)

        yhat = float(pred.predict(X)[0])
        yhat = max(0.0, yhat)

        vol.loc[pd.to_datetime(dt).normalize()] = yhat
        updates[pd.to_datetime(dt).normalize()] = yhat

    s_pred = pd.Series(updates).sort_index()
    s_pred = s_pred[(s_pred.index >= start_ts) & (s_pred.index <= end_ts)]

    if s_pred.empty:
        raise RuntimeError("registered_plus: prediction result is empty")

    df_w = s_pred.rename("forecast_volume").reset_index().rename(columns={"index": "date"})
    df_w["date"] = pd.to_datetime(df_w["date"]).dt.normalize()
    df_w["forecast_volume"] = df_w["forecast_volume"].astype(float).clip(lower=0.0).round().astype(int)

    out = df_w.copy()
    out["mail_kind"] = "registered_plus"
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["date", "mail_kind", "forecast_volume"]]


def _forecast_nenga_assembly(*, office_id: int, start_ts: pd.Timestamp, days: int, eng) -> pd.DataFrame:
    import posms.models.nenga.assembly as nenga_asm

    end_ts = start_ts + pd.Timedelta(days=days - 1)
    y = nenga_asm.predict(eng, office_id=office_id, round_to_1000=True)  # 全期間の配列

    # NengaFeatureBuilder の build と同じ date 列で合わせる
    from posms.models.nenga.features import NengaFeatureBuilder
    df = NengaFeatureBuilder(eng, office_id=office_id, mail_kind="nenga_assembly").build()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date").reset_index(drop=True)

    if len(y) != len(df):
        raise RuntimeError(f"nenga_assembly: preds length mismatch df. preds={len(y)}, df={len(df)}")

    out = pd.DataFrame(
        {"date": df["date"], "mail_kind": "nenga_assembly", "forecast_volume": pd.Series(y).astype(int)}
    )
    out = out[(out["date"] >= start_ts) & (out["date"] <= end_ts)].copy()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["date", "mail_kind", "forecast_volume"]]


def _forecast_nenga_delivery(*, office_id: int, start_ts: pd.Timestamp, days: int, eng) -> pd.DataFrame:
    import posms.models.nenga.delivery as nenga_del

    end_ts = start_ts + pd.Timedelta(days=days - 1)
    y = nenga_del.predict(eng, office_id=office_id)  # 全期間の配列

    from posms.models.nenga.features import NengaFeatureBuilder
    df = NengaFeatureBuilder(eng, office_id=office_id, mail_kind="nenga_delivery").build()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date").reset_index(drop=True)

    if len(y) != len(df):
        raise RuntimeError(f"nenga_delivery: preds length mismatch df. preds={len(y)}, df={len(df)}")

    out = pd.DataFrame(
        {"date": df["date"], "mail_kind": "nenga_delivery", "forecast_volume": pd.Series(y).astype(int)}
    )
    out = out[(out["date"] >= start_ts) & (out["date"] <= end_ts)].copy()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["date", "mail_kind", "forecast_volume"]]


def main() -> int:
    ap = argparse.ArgumentParser(description="POSMS forecast entry (exe target)")
    ap.add_argument("--csvdir", required=True, help="export_csv directory from Excel")
    # 互換のため残してもよい（使わないなら削除OK）
    ap.add_argument("--sqlite", default="", help="(unused) SQLite db path (optional)")
    args = ap.parse_args()

    csvdir = Path(args.csvdir)
    if not csvdir.exists():
        print(f"ERROR: csvdir not found: {csvdir}", file=sys.stderr)
        return 2

    inputs = _load_inputs(csvdir)
    start_ts = pd.to_datetime(inputs.get("start"), errors="coerce").normalize()
    if pd.isna(start_ts):
        print(f"ERROR: invalid start in inputs.json: {inputs.get('start')!r}", file=sys.stderr)
        return 3

    days = int(inputs.get("days", 28))
    office_id = int(inputs.get("office_id", 1))

    # ------------------------------------------------------------
    # SQLite 固定パス（excel_templates/posms_demo.db）
    #   csvdir = .../excel_templates/export_csv を想定
    #   その親（export_csvの親=excel_templates）にDBがある
    # ------------------------------------------------------------
    excel_templates_dir = csvdir.parent  # = .../excel_templates
    db_path = excel_templates_dir / "posms_demo.db"
    if not db_path.exists():
        print(f"ERROR: SQLite file not found: {db_path}", file=sys.stderr)
        return 4

    os.environ["DATABASE_URL"] = f"sqlite:///{db_path.resolve().as_posix()}"

    # FeatureBuilder の仕様に合わせて office_id / engine を確定
    from posms.features.builder import FeatureBuilder
    fb_base = FeatureBuilder(office_id=office_id, mail_kind="normal")
    eng = fb_base.engine
    _ = fb_base._load_mail()
    office_id = int(fb_base.office_id)

    kinds = _choose_mail_kinds(start_ts.date(), days)

    frames: list[pd.DataFrame] = []
    for k in kinds:
        if k == "normal":
            frames.append(_forecast_normal(office_id=office_id, start_ts=start_ts, days=days, eng=eng))
        elif k == "registered_plus":
            frames.append(_forecast_registered_plus(office_id=office_id, start_ts=start_ts, days=days, eng=eng))
        elif k == "nenga_assembly":
            frames.append(_forecast_nenga_assembly(office_id=office_id, start_ts=start_ts, days=days, eng=eng))
        elif k == "nenga_delivery":
            frames.append(_forecast_nenga_delivery(office_id=office_id, start_ts=start_ts, days=days, eng=eng))
        else:
            raise RuntimeError(f"unsupported mail_kind: {k}")

    out = pd.concat(frames, ignore_index=True).sort_values(["date", "mail_kind"])
    out_path = csvdir / "forecast.csv"
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"OK: wrote forecast.csv -> {out_path}")
    print(f"mail_kinds: {kinds}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())