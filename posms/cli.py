# posms/cli.py
"""
posms.cli
=========

Command-line interface for the Postal Operation Shift-Management System.

Usage examples
--------------

# 予測 + シフト最適化 (E2E)
poetry run posms run-monthly --predict-date 2025-08-01

# 予測モデルだけ再学習
poetry run posms train --office-id 1

# 指定開始日から4週間の予測をDBへ書き戻し
poetry run posms forecast --office-id 1 --start 2025-09-08 --days 28

# シフトだけ再最適化（分担表案）
poetry run posms optimize --date 2025-08-01 --output-type 分担表案
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
import jpholiday
import typer
from sqlalchemy import create_engine, inspect, text
from typing import Annotated

from posms.features import builder as FB
from posms.features.builder import FEATURE_COLUMNS, FeatureBuilder
try:
    from posms.flows.monthly_flow import monthly_refresh as _monthly
except ImportError:
    from posms.flows.monthly_flow import monthly_train as _monthly
from posms.models import ModelPredictor
from posms.models.trainer import ModelTrainer
from posms.optimization.shift_builder import OutputType, ShiftBuilder

app = typer.Typer(help="Postal Operation Shift-Management System CLI")


# ---------- Helper -------------------------------------------------
def _default_template() -> Path:
    return Path("excel_templates/shift_template.xlsx")


def _make_engine_from_env():
    """DATABASE_URL または POSTGRES_* から DB 接続（ゼロ設定）"""
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return create_engine(db_url, future=True, pool_pre_ping=True)

    user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
    pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    name = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")
    if not all([user, pwd, name]):
        raise RuntimeError("DB接続情報が不足：DATABASE_URL または POSTGRES_* を設定してください")
    return create_engine(
        f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}",
        future=True,
        pool_pre_ping=True,
    )


def _resolve_mailvolume_table(eng) -> str:
    insp = inspect(eng)
    existing = {t.lower() for t in insp.get_table_names(schema="public")}
    if "mailvolume" in existing:
        return "mailvolume"
    if "mail_volume" in existing:
        return "mail_volume"
    return '"MailVolume"'  # 引用付きで作成された場合


def _season(ts: pd.Timestamp) -> int:
    """1:春(3-5), 2:夏(6-8), 3:秋(9-11), 4:冬(12-2)"""
    m = ts.month
    return 1 if m in (3, 4, 5) else 2 if m in (6, 7, 8) else 3 if m in (9, 10, 11) else 4

def _is_hol(ts: pd.Timestamp) -> int:
    return int(jpholiday.is_holiday(ts.date()))


# ---------- Commands ----------------------------------------------
@app.command("run-monthly")
def run_monthly(
    predict_date: Annotated[
        str,
        typer.Option(
            "--predict-date",
            "-d",
            help="YYYY-MM-DD 形式。省略時は今日。",
        ),
    ] = "",
    output_type: Annotated[
        OutputType,
        typer.Option(
            "--output-type",
            "-o",
            case_sensitive=False,
            help="Excel 出力の種類: 分担表 / 勤務指定表 / 分担表案",
        ),
    ] = OutputType.分担表,
    excel_template: Annotated[
        Path,
        typer.Option("--template", "-t", help="Excel テンプレート .xlsx"),
    ] = _default_template(),
):
    """
    1. Excel → DB 取込
    2. XGBoost 再学習 (MLflow ログ)
    3. 需要予測 → PuLP シフト最適化
    4. Excel に書き出し
    """
    params = {
        "predict_date": predict_date or str(date.today()),
        "output_type": output_type.value,
        "excel_template": str(excel_template),
    }
    _monthly(**params)  # Prefect Flow/関数を実行（refresh or train）


@app.command("train")
def train_model(
    office_id: Annotated[int | None, typer.Option("--office-id", help="局ID（1局のみなら省略可）")] = None,
    n_estimators: Annotated[int, typer.Option(help="XGBoost 木数")] = 200,
    max_depth: Annotated[int, typer.Option(help="XGBoost 木の深さ")] = 6,
):
    """DBのMailVolumeを使ってモデルを学習し、MLflowに保存。run_id を出力。"""
    fb = FeatureBuilder(office_id=office_id)
    X, y = fb.build()
    run_id = ModelTrainer({"n_estimators": n_estimators, "max_depth": max_depth}).train(X, y)
    typer.echo(f"MLflow run_id: {run_id}")


@app.command("forecast")
def forecast_4weeks(
    start: Annotated[str, typer.Option("--start", "-s", help="予測開始日 YYYY-MM-DD")],
    days: Annotated[int, typer.Option("--days", "-n", help="予測日数（既定=28）")] = 28,
    office_id: Annotated[int | None, typer.Option("--office-id", help="局ID（1局のみなら省略可）")] = None,
    run_id: Annotated[str | None, typer.Option("--run-id", help="使用する学習 run_id。未指定なら Production/最新run")] = None,
    stage: Annotated[str, typer.Option("--stage", help="Model Registry ステージ名")] = "Production",
):
    """
    学習済みモデルで、指定開始日から days 日分のローリング予測を行い、
    MailVolume.forecast_volume を更新します。
    """
    # 特徴量（DBから）
    fb = FeatureBuilder(office_id=office_id)
    eng = fb.engine
    MV = _resolve_mailvolume_table(eng)
    if office_id is None:
        try:
            # FeatureBuilder が中で解決してくれていれば使う
            if getattr(fb, "office_id", None) is not None:
                office_id = int(fb.office_id)
            else:
                from sqlalchemy import text as _t
                with eng.begin() as con:
                    rows = con.execute(_t(f"SELECT DISTINCT office_id FROM {MV} ORDER BY office_id")).fetchall()
                if len(rows) == 1:
                    office_id = int(rows[0][0])
                elif len(rows) == 0:
                    typer.secho("MailVolume にレコードがありません。先に学習用データを投入してください。", err=True)
                    raise typer.Exit(code=1)
                else:
                    ids = ", ".join(str(r[0]) for r in rows)
                    typer.secho(f"複数の office_id が見つかりました: [{ids}]  --office-id を指定してください。", err=True)
                    raise typer.Exit(code=2)
        except Exception as e:
            typer.secho(f"office_id 自動解決に失敗: {e}", err=True)
            raise typer.Exit(code=3)

    # 期間の行が無ければ作る（actual=NULL, forecast=NULL, price_increase_flagは既存値/無ければ0）
    start_ts = pd.to_datetime(start).normalize()
    targets = pd.date_range(start_ts, periods=days, freq="D")

    with eng.begin() as con:
        df_flags = pd.read_sql(
            text(f'SELECT "date", price_increase_flag FROM {MV} WHERE office_id=:o AND "date" BETWEEN :d1 AND :d2'),
            con,
            params={"o": office_id, "d1": targets[0], "d2": targets[-1]},
            parse_dates=["date"],
        ).set_index("date")
        for dt in targets:
            val = df_flags["price_increase_flag"].get(dt) if not df_flags.empty else None
            if isinstance(val, (bool, np.bool_)):
            	flag = bool(val)
            elif val is None or (isinstance(val, float) and np.isnan(val)):  # 欠損など
                flag = False
            else:
            	# 0/1 や '0'/'1' が来ても安全に解釈
            	flag = bool(int(val))
            con.execute(
                text(
                    f'INSERT INTO {MV} ("date", office_id, actual_volume, forecast_volume, price_increase_flag) '
                    f'VALUES (:d, :o, NULL, NULL, :f) '
                    f'ON CONFLICT ("date", office_id) DO NOTHING'
                ),
                {"d": dt.date(), "o": int(office_id), "f": flag},
            )

    # 既存の系列（actual）で進め、未来は予測で埋めながらローリング
    hist = fb._load_mail().set_index("date").sort_index()
    vol = hist["actual_volume"].astype("float").copy()
    predictor = ModelPredictor(run_id=run_id, stage=stage)

    updates: list[tuple[date, int]] = []
    for ts in targets:
        prev_1 = ts - pd.Timedelta(days=1)
        prev_7 = ts - pd.Timedelta(days=7)
        if prev_1 not in vol.index or prev_7 not in vol.index:
            continue
        last7 = vol.loc[ts - pd.Timedelta(days=7) : ts - pd.Timedelta(days=1)]
        if len(last7) < 7 or last7.isna().any():
            continue

        flag = int(hist["price_increase_flag"].get(ts, 0)) if "price_increase_flag" in hist.columns else 0
        X_row = pd.DataFrame(
            [
                {
                    "dow": ts.weekday(),
                    "dow_sin": np.sin(2 * np.pi * ts.weekday() / 7.0),
                    "dow_cos": np.cos(2 * np.pi * ts.weekday() / 7.0),
                    "is_holiday": _is_hol(ts),
                    "is_after_holiday": _is_hol(ts - pd.Timedelta(days=1)),
                    "is_after_after_holiday": _is_hol(ts - pd.Timedelta(days=2)),
                    "month": ts.month,
                    "season": _season(ts),
                    "lag_1": float(vol.loc[prev_1]),
                    "lag_7": float(vol.loc[prev_7]),
                    "rolling_mean_7": float(last7.mean()),
                    "is_new_year": int(ts.month == 1 and 1 <= ts.day <= 3),
                    "is_obon": int(ts.month == 8 and 13 <= ts.day <= 16),
                    "price_increase_flag": flag,
                }
            ]
        )[FEATURE_COLUMNS]

        yhat_raw = float(predictor.predict(X_row)[0])
        # 負の予測は 0 にクリップ（count系のため）
        yhat = max(0.0, yhat_raw)
        vol.loc[ts] = yhat  # 次日の特徴量計算のために予測値を採用
        updates.append((ts.date(), int(round(yhat))))

    # 書き戻し
    if updates:
        with eng.begin() as con:
            for d, v in updates:
                con.execute(
                    text(f'UPDATE {MV} SET forecast_volume=:v, updated_at=NOW() WHERE "date"=:d AND office_id=:o'),
                    {"v": v, "d": d, "o": int(office_id)},
                )
        typer.echo(
            f"forecast_volume 更新: {len(updates)} 件（office_id={office_id}, {targets[0].date()}〜{targets[-1].date()}）"
        )
    else:
        typer.echo("更新対象なし（直近実績不足 or 行未作成）")

    # 参考：MLflow保存先
    typer.echo(f"MLflow: {mlflow.get_tracking_uri()}")


@app.command("optimize")
def optimize_shift(
    date_str: Annotated[str, typer.Option("--date", "-d")] = str(date.today()),
    output_type: Annotated[
        OutputType,
        typer.Option(
            help="分担表 / 勤務指定表 / 分担表案",
            case_sensitive=False,
        ),
    ] = OutputType.分担表,
    template: Annotated[
        Path,
        typer.Option("--template", "-t", help="Excel テンプレート"),
    ] = _default_template(),
):
    """需要予測済みデータを入力にシフトのみ再最適化"""
    demand = FeatureBuilder().predict(date_str)  # 既定 run_id を内部でロード
    staff = FeatureBuilder().load_staff()
    out = ShiftBuilder(template).build(demand, staff, output_type)
    typer.echo(f"Excel saved → {out.resolve()}")


if __name__ == "__main__":
    app()
