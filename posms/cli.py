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
from shutil import copyfile
from sqlalchemy import inspect, text
from typing import Annotated, Optional
from .exporters.excel_exporter import write_dataframe_to_excel
from posms.utils.db import SessionManager
from posms.features import builder as FB
from posms.features.builder import FEATURE_COLUMNS, FeatureBuilder
try:
    from posms.flows.monthly_flow import monthly_refresh as _monthly
except ImportError:
    from posms.flows.monthly_flow import monthly_train as _monthly
from posms.models import ModelPredictor
from posms.models.trainer import ModelTrainer
#from posms.optimization.shift_builder import OutputType, ShiftBuilder

app = typer.Typer(help="Postal Operation Shift-Management System CLI")
#既存の try/except ～ ダミー定義を丸ごと置き換え
from enum import Enum

try:
    from posms.optimization.shift_builder import OutputType  # 本物（あれば）
except Exception:
    class OutputType(str, Enum):  # Typerが扱える
        分担表 = "分担表"
        勤務指定表 = "勤務指定表"
        分担表案 = "分担表案"


# ---------- Helper -------------------------------------------------
def _default_template() -> Path:
    return Path("excel_templates/shift_template.xlsx")

def _engine():
    """SessionManager 経由で Engine を取得（Postgres⇄SQLite 自動切替）"""
    return SessionManager().engine

#def _engine():
#    """DATABASE_URL または POSTGRES_* から DB 接続（ゼロ設定）"""
#    db_url = os.getenv("DATABASE_URL")
#    if db_url:
#        return create_engine(db_url, future=True, pool_pre_ping=True)
#
#    user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
#    pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
#    host = os.getenv("POSTGRES_HOST", "localhost")
#    port = os.getenv("POSTGRES_PORT", "5432")
#    name = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")
#    if not all([user, pwd, name]):
#        raise RuntimeError("DB接続情報が不足：DATABASE_URL または POSTGRES_* を設定してください")
#    return create_engine(
#        f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}",
#        future=True,
#        pool_pre_ping=True,
#    )

def _engine():
    return SessionManager().engine
    
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
    
def _prepare_out_from_template(out: Path, template: Path) -> None:
    """
    out が無ければ、template (xlsm) をそのままコピーして作る。
    既に out があれば何もしない（テンプレの分担予定表(案)やマクロを保持）。
    """
    out = Path(out)
    template = Path(template)
    out.parent.mkdir(parents=True, exist_ok=True)
    if not out.exists():
        if not template.exists():
            raise FileNotFoundError(f"Template not found: {template}")
        copyfile(template, out)


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
    from posms.optimization.shift_builder import ShiftBuilder
    """需要予測済みデータを入力にシフトのみ再最適化"""
    demand = FeatureBuilder().predict(date_str)  # 既定 run_id を内部でロード
    staff = FeatureBuilder().load_staff()
    out = ShiftBuilder(template).build(demand, staff, output_type)
    typer.echo(f"Excel saved → {out.resolve()}")
    
@app.command("export-excel")
def export_excel(
    sql: str = typer.Option(None, "--sql", help="実行するSQL。--query-fileとどちらか必須"),
    query_file: Path = typer.Option(None, "--query-file", exists=True, help="SQLファイルパス。--sqlとどちらか必須"),
    out: Path = typer.Option(Path("dist/shift_report.xlsx"), "--out", help="出力先 .xlsx"),
    sheet: str = typer.Option("export", "--sheet", help="書き込み先シート名"),
    template: Path = typer.Option(None, "--template", help="テンプレ .xlsx（未指定なら空ブック）"),
    start_cell: str = typer.Option("A1", "--start-cell", help="開始セル（例:A1）"),
    header_map: str | None = typer.Option(
        None,
        "--header-map",
        help="英=日 をカンマ区切りで指定。例: employee_code=社員コード,name=氏名,team_name=班",
    ),
    append: bool = typer.Option(False, "--append", help="既存の .xlsx にシートを追記"),
    sort_by: str | None = typer.Option(None, "--sort-by", help="カンマ区切りの列名で昇順ソート"),
    sort_natural: str | None = typer.Option(None, "--sort-natural", help="自然順ソートする列名（例: 社員番号, zone_code）"),
):
    """
    任意のSQL結果をテンプレ(任意)へ「値だけ」書き込んで .xlsx を生成する。
    相手PCは“開くだけ”。ODBC/PowerQuery/マクロは不要。
    """
    if not sql and not query_file:
        typer.echo("ERROR: --sql または --query-file のいずれかを指定してください。")
        raise typer.Exit(code=2)

    if query_file and not sql:
        sql = Path(query_file).read_text(encoding="utf-8")

    # header_map 解析
    header_map_dict = None
    if header_map:
        header_map_dict = {}
        for kv in header_map.split(","):
            if "=" in kv:
                k, v = kv.split("=", 1)
                header_map_dict[k.strip()] = v.strip()

    engine = _engine()  # DATABASE_URL から接続
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
        
    if sort_by:
        cols = [c.strip() for c in sort_by.split(",") if c.strip()]
        df = df.sort_values(cols)

    if sort_natural:
        import re
        col = sort_natural.strip()
        if col in df.columns:
            # 数字抽出→int化（無ければ0）で自然順
            key = (
                df[col]
                .astype(str)
                .str.extract(r"(\d+)", expand=False)
                .fillna("0").astype(int)
            )
            df = df.assign(__key__=key).sort_values(["__key__", col]).drop(columns="__key__")

    write_dataframe_to_excel(
        df=df,
        out_path=Path(out),
        sheet_name=sheet,
        template_path=Path(template) if template else None,
        header_map=header_map_dict,
        start_cell=start_cell,
        append=append,
    )
    typer.echo(f"✅ Exported: {out}")

@app.command("export-sheet-employees")
def export_sheet_employees(
    department_code: str = typer.Option(..., "--department-code", "-dc", help="例: DPT-A もしくは 部署名"),
    team: str = typer.Option(..., "--team", "-t", help="例: 1班"),
    out: Path = typer.Option(Path("excel_out/班データ.xlsx"), "--out"),
    sheet: str = typer.Option("社員", "--sheet"),
    template: Path = typer.Option(None, "--template"),
):
    eng = _engine()

    # department_code カラムの有無で WHERE を切り替え
    insp = inspect(eng)
    dept_cols = {c["name"] if isinstance(c, dict) else c.name for c in insp.get_columns("department")}
    has_code = "department_code" in dept_cols

    # Postgres / SQLite 共通SQL（DB依存の ORDER BY は付けない）
    base_sql = """
      SELECT
          e.employee_code   AS "社員番号",
          e.name            AS "氏名",
          d.department_name AS "部",
          t.team_name       AS "班",
          COALESCE(e.employment_type,'')  AS "社員タイプ",
          COALESCE(e.position,'')         AS "役職",
          CASE WHEN COALESCE(e.is_leader,0)      <> 0 THEN 1 ELSE 0 END AS "班長",
          CASE WHEN COALESCE(e.is_vice_leader,0) <> 0 THEN 1 ELSE 0 END AS "副班長",
          CASE WHEN COALESCE(e.is_certifier,0)   <> 0 THEN 1 ELSE 0 END AS "認証司",
          COALESCE(e.default_work_hours,0)  AS "勤務時間(日)",
          COALESCE(e.monthly_work_hours,0)  AS "勤務時間(月)"
      FROM employee e
      JOIN team t ON t.team_id = e.team_id
      JOIN department d ON d.department_id = t.department_id
      WHERE {where_clause}
    """

    if has_code:
        where_clause = "(COALESCE(d.department_code,'') = :dc OR d.department_name = :dc) AND t.team_name = :tn"
    else:
        where_clause = "d.department_name = :dc AND t.team_name = :tn"

    sql = base_sql.format(where_clause=where_clause)

    with eng.connect() as con:
        df = pd.read_sql(text(sql), con, params={"dc": department_code, "tn": team})

    # 自然順ソート：社員番号の数字部分で並び替え（DB依存を避ける）
    if "社員番号" in df.columns:
        df["__num__"] = (
            df["社員番号"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("999999999")
            .astype(int)
        )
        df = df.sort_values(["__num__", "社員番号"]).drop(columns="__num__")

    # 0/1 列は int に統一（見栄え＆型ブレ防止）
    for c in ["班長", "副班長", "認証司", "勤務時間(日)", "勤務時間(月)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # 出力
    out.parent.mkdir(parents=True, exist_ok=True)
    write_dataframe_to_excel(
        df=df,
        out_path=out,
        sheet_name=sheet,
        template_path=template,
        start_cell="A1",
        append=bool(template),
    )
    typer.echo(f"✅ 社員シート {len(df)}行 → {out}（{sheet}）")

@app.command("export-sheet-zones")
def export_sheet_zones(
    department_code: str = typer.Option(..., "--department-code", "-dc"),
    team: str = typer.Option(..., "--team", "-t"),
    out: Path = typer.Option(Path("excel_out/班データ.xlsx"), "--out"),
    sheet: str = typer.Option("区情報", "--sheet"),
    template: Path = typer.Option(None, "--template"),
):
    eng = _engine()

    insp = inspect(eng)
    dept_cols = {c["name"] if isinstance(c, dict) else c.name for c in insp.get_columns("department")}
    has_code = "department_code" in dept_cols

    base_sql = """
      SELECT
        CAST(COALESCE(z.zone_code, 'Z' || z.zone_id) AS TEXT) AS "区コード",
        COALESCE(z.zone_name,'')     AS "区名",
        t.team_name                  AS "班",
        COALESCE(z.operational_status,'通配') AS "稼働",
        COALESCE(dem.demand_mon,0)     AS "月",
        COALESCE(dem.demand_tue,0)     AS "火",
        COALESCE(dem.demand_wed,0)     AS "水",
        COALESCE(dem.demand_thu,0)     AS "木",
        COALESCE(dem.demand_fri,0)     AS "金",
        COALESCE(dem.demand_sat,0)     AS "土",
        COALESCE(dem.demand_sun,0)     AS "日",
        COALESCE(dem.demand_holiday,0) AS "祝"
      FROM zone z
      JOIN team t          ON t.team_id = z.team_id
      JOIN department d    ON d.department_id = t.department_id
      LEFT JOIN demandprofile dem ON dem.zone_id = z.zone_id
      WHERE {where_clause}
      ORDER BY COALESCE(z.zone_code, 'Z' || z.zone_id)
    """

    if has_code:
        where_clause = "(COALESCE(d.department_code,'') = :dc OR d.department_name = :dc) AND t.team_name = :tn"
    else:
        where_clause = "d.department_name = :dc AND t.team_name = :tn"

    sql = base_sql.format(where_clause=where_clause)

    with eng.connect() as con:
        df = pd.read_sql(text(sql), con, params={"dc": department_code, "tn": team})

    out.parent.mkdir(parents=True, exist_ok=True)
    write_dataframe_to_excel(df, out, sheet_name=sheet, template_path=template,
                             start_cell="A1", append=bool(template))
    typer.echo(f"✅ 区情報シート {len(df)}行 → {out}（{sheet}）")

@app.command("export-sheet-employee-demand")
def export_sheet_employee_demand(
    team: str = typer.Option(..., "--team", "-t", help="例: 1班"),
    out: Path = typer.Option(Path("excel_out/班データ.xlsx"), "--out"),
    template: Path = typer.Option(None, "--template", help="xls/xlsx/xlsm（xlsmはマクロ温存）"),
    sheet: str = typer.Option("社員別需要", "--sheet"),
):
    """
    班ごとの『社員別需要』シートをExcelに出力:
      列 = 社員番号, 氏名, <区名...横展開>, 月, 火, 水, 木, 金, 土, 日, 祝
      値 = 区列: EmployeeZoneProficiency.proficiency（無い所は0） / 曜日列: 1/0
    """
    eng = _engine()

    # 1) 班の区一覧（Postgres/SQLite 共通）
    sql_zones = """
      SELECT z.zone_id, z.zone_code, z.zone_name
        FROM zone z
        JOIN team t ON z.team_id = t.team_id
       WHERE t.team_name = :team
       ORDER BY COALESCE(z.zone_code, 'Z' || z.zone_id)
    """

    # 2) 社員＋曜日可否（0/1 前提・DB共通 / 並びは後で pandas で自然順に）
    sql_emp = """
      SELECT e.employee_id, e.employee_code, e.name,
             CASE WHEN COALESCE(ea.available_mon,  1) <> 0 THEN 1 ELSE 0 END AS mon,
             CASE WHEN COALESCE(ea.available_tue,  1) <> 0 THEN 1 ELSE 0 END AS tue,
             CASE WHEN COALESCE(ea.available_wed,  1) <> 0 THEN 1 ELSE 0 END AS wed,
             CASE WHEN COALESCE(ea.available_thu,  1) <> 0 THEN 1 ELSE 0 END AS thu,
             CASE WHEN COALESCE(ea.available_fri,  1) <> 0 THEN 1 ELSE 0 END AS fri,
             CASE WHEN COALESCE(ea.available_sat,  0) <> 0 THEN 1 ELSE 0 END AS sat,
             CASE WHEN COALESCE(ea.available_sun,  0) <> 0 THEN 1 ELSE 0 END AS sun,
             CASE WHEN COALESCE(ea.available_hol,  0) <> 0 THEN 1 ELSE 0 END AS hol
        FROM employee e
        JOIN team t ON e.team_id = t.team_id
   LEFT JOIN employeeavailability ea ON ea.employee_id = e.employee_id
       WHERE t.team_name = :team
    """

    # 3) 熟練度（社員×区）も共通
    sql_prof = """
      SELECT ezp.employee_id, z.zone_name, COALESCE(ezp.proficiency, 0) AS proficiency
        FROM employeezoneproficiency ezp
        JOIN zone z ON z.zone_id = ezp.zone_id
        JOIN team t ON z.team_id = t.team_id
       WHERE t.team_name = :team
    """

    from sqlalchemy import text
    import pandas as pd

    with eng.connect() as con:
        df_z = pd.read_sql(text(sql_zones), con, params={"team": team})
        df_e = pd.read_sql(text(sql_emp),   con, params={"team": team})
        df_p = pd.read_sql(text(sql_prof),  con, params={"team": team})

    # 列名小文字化
    for df in (df_z, df_e, df_p):
        df.columns = [c.lower() for c in df.columns]

    # 社員コードの“自然順”は常に pandas 側で（DB依存の正規表現等を使わない）
    if "employee_code" in df_e.columns:
        df_e["__num__"] = (
            df_e["employee_code"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)   # 数字を抽出
            .fillna("999999999")
            .astype(int)
        )
        df_e = df_e.sort_values(["__num__", "employee_code"]).drop(columns="__num__")

    # 区名の並び（空でも列は用意）
    zone_cols = df_z["zone_name"].dropna().astype(str).tolist()

    # 熟練度を横展開
    if df_p.empty:
        wide = pd.DataFrame(columns=["employee_id"] + zone_cols)
    else:
        wide = (
            df_p.pivot_table(index="employee_id", columns="zone_name",
                             values="proficiency", fill_value=0, aggfunc="max")
            .reset_index()
            .rename_axis(None, axis=1)
        )

    # 結合 → 欠損0埋め
    df = df_e.merge(wide, on="employee_id", how="left")
    for zc in zone_cols:
        if zc not in df.columns:
            df[zc] = 0
        df[zc] = pd.to_numeric(df[zc], errors="coerce").fillna(0).astype("int64")

    for c in ["mon","tue","wed","thu","fri","sat","sun","hol"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    # 列順と日本語見出し
    ordered = ["employee_code", "name"] + zone_cols + ["mon","tue","wed","thu","fri","sat","sun","hol"]
    ordered = [c for c in df.columns if c in ordered]  # 安全化
    df = df[ordered].rename(columns={
        "employee_code":"社員番号","name":"氏名",
        "mon":"月","tue":"火","wed":"水","thu":"木","fri":"金","sat":"土","sun":"日","hol":"祝",
    })

    # 出力
    out.parent.mkdir(parents=True, exist_ok=True)
    write_dataframe_to_excel(
        df=df,
        out_path=out,
        sheet_name=sheet,
        template_path=template,
        start_cell="A1",
        append=bool(template) or out.exists(),
    )
    typer.echo(f"✅ 社員別需要シート {len(df)}行 → {out}（{sheet}）")

@app.command("export-sheet-jobtypes-regular")
def export_sheet_jobtypes_regular(
    out: Path = typer.Option(Path("excel_out/班データ統合.xlsx"), "--out"),
    template: Path = typer.Option(None, "--template"),
    sheet: str = typer.Option("勤務種別(正社員)", "--sheet"),
):
    eng = _engine()
    sql = """
      SELECT job_name  AS "勤務名",
             start_time AS "就労開始時間",
             end_time   AS "就労終了時間",
             work_hours AS "勤務時間"
        FROM JobType
       WHERE lower(COALESCE(classification,'')) IN ('reg','regular','fulltime','正社員')
       ORDER BY start_time, job_name;
    """
    with eng.connect() as con:
        df = pd.read_sql(text(sql), con)
    out.parent.mkdir(parents=True, exist_ok=True)
    write_dataframe_to_excel(df, out, sheet_name=sheet, template_path=template,
                             start_cell="A1", append=bool(template) or out.exists())
    typer.echo(f"✅ 勤務種別(正社員) {len(df)}行 → {out}（{sheet}）")

@app.command("export-sheet-jobtypes-fixedterm")
def export_sheet_jobtypes_fixedterm(
    out: Path = typer.Option(Path("excel_out/班データ統合.xlsx"), "--out"),
    template: Path = typer.Option(None, "--template"),
    sheet: str = typer.Option("勤務種別(期間雇用)", "--sheet"),
):
    eng = _engine()
    sql = """
      SELECT job_name  AS "勤務名",
             start_time AS "就労開始時間",
             end_time   AS "就労終了時間",
             work_hours AS "勤務時間"
        FROM JobType
       WHERE lower(COALESCE(classification,'')) IN ('contract','part-time','pt','temp','dispatch','期間雇用社員')
       ORDER BY start_time, job_name;
    """
    with eng.connect() as con:
        df = pd.read_sql(text(sql), con)
    out.parent.mkdir(parents=True, exist_ok=True)
    write_dataframe_to_excel(df, out, sheet_name=sheet, template_path=template,
                             start_cell="A1", append=bool(template) or out.exists())
    typer.echo(f"✅ 勤務種別(期間雇用) {len(df)}行 → {out}（{sheet}）")
    
@app.command("export-team-workbook")
def export_team_workbook(
    department_code: str = typer.Option(..., "--department-code", "-dc", help="例: DPT-A"),
    team: str = typer.Option(..., "--team", "-t", help="班名（例: 1班）"),
    out: Path = typer.Option(Path("excel_templates/班統合データ.xlsm"), "--out", help="出力先 .xlsm"),
    template: Path = typer.Option(
        Path("excel_templates/shift_template.xlsm"),
        "--template",
        help="テンプレート（xlsmを指定すればマクロ保持。分担予定表(案)が入っていること）",
    ),
    db_url: Annotated[str | None, typer.Option("--db-url")] = None,
    sqlite: Annotated[Path | None, typer.Option("--sqlite")] = None,
    plan_sheet_name: Annotated[str, typer.Option("--plan-sheet-name", help="テンプレ内のシート名確認用")] = "分担予定表(案)",
):
    """
    部署・班ごとの Excel ブックをテンプレ込みで統合出力。
    シート構成（テンプレ保持）：
      - 分担予定表(案)  ← テンプレからそのまま残す
      - 社員 / 区情報 / 社員別需要 / 正社員服務表 / 期間雇用社員服務表
    """
    typer.echo(f"▶ 班「{team}」の統合Excelを作成中...")

    # 0) テンプレから out を作って「分担予定表(案)」を取り込む（マクロ含め全保持）
    _prepare_out_from_template(out, template)
    typer.echo(f"  - テンプレ基盤: {template} → {out}")

    # （任意）テンプレ内に指定シートが入っているか軽く検証（openpyxl keep_vba があれば検査可能）
    try:
        import openpyxl
        wb = openpyxl.load_workbook(out, keep_vba=True, read_only=True)
        if plan_sheet_name not in wb.sheetnames:
            typer.secho(f"WARNING: 出力ブックに '{plan_sheet_name}' シートが見つかりません。テンプレの確認をしてください。", err=True)
        wb.close()
    except Exception:
        # openpyxl が無くても処理自体は進める（コピー済みなので問題なし）
        pass

    # ここからは out をベースに「追記」していく
    # 1️⃣ 社員
    export_sheet_employees(
        department_code=department_code,
        team=team,
        out=out,
        sheet="社員",
        template=out,  # ← out に追記
    )

    # 2️⃣ 区情報
    export_sheet_zones(
        department_code=department_code,
        team=team,
        out=out,
        sheet="区情報",
        template=out,
    )

    # 3️⃣ 社員別需要
    export_sheet_employee_demand(
        team=team,
        out=out,
        template=out,
        sheet="社員別需要",
    )

    # 4️⃣ 正社員服務表
    export_sheet_jobtypes_regular(out=out, template=out, sheet="正社員服務表")

    # 5️⃣ 期間雇用社員服務表
    export_sheet_jobtypes_fixedterm(out=out, template=out, sheet="期間雇用社員服務表")

    typer.echo(f"✅ 統合完了: {out.resolve()}")
    
@app.command("import-excel")
def import_excel(
    file: Path = typer.Option(..., "--file", exists=True, help="編集済みの班ファイル .xlsx")
):
    """
    Excel から DB へ取り込み（UPSERT）
    - C) 社員: employee（部/班マスタも補完）
    - B) 区情報: zone → demandprofile
    - A) 社員別需要: employeeavailability / employeezoneproficiency
    """
    eng = _engine()
    # 一度だけオープンして使い回す
    xls = pd.ExcelFile(file)

    # ファイル名から班名を推定（例: 1班データ.xlsx → 1班）
    import re
    m = re.search(r"(\d+)班", file.name)
    team_name = f"{m.group(1)}班" if m else None

    with eng.begin() as con:
        # SQLite はロック耐性を上げる（書き込み頻度が高い前提）
        if eng.url.get_backend_name() == "sqlite":
            con.exec_driver_sql("PRAGMA journal_mode=WAL;")
            con.exec_driver_sql("PRAGMA busy_timeout=5000;")  # 5秒まで待つ

        # ---------- C) 社員（employee を社員番号で更新/追加） ----------
        if "社員" in xls.sheet_names:
            df_emp = pd.read_excel(xls, sheet_name="社員").rename(columns={
                "社員番号": "employee_code",
                "氏名": "name",
                "部": "department_name",
                "班": "team_name",
                "社員タイプ": "employment_type",
                "役職": "position",
                "班長": "is_leader",
                "副班長": "is_vice_leader",
                "認証司": "is_certifier",
                "勤務時間(日)": "default_work_hours",
                "勤務時間(月)": "monthly_work_hours",
            })

            # 前処理：空→空文字、左右スペース除去
            for c in ["department_name", "team_name"]:
                if c in df_emp.columns:
                    df_emp[c] = df_emp[c].fillna("").astype(str).str.strip()

            # 1) department / team を補完（無ければ作る）
            dep_names = sorted({n for n in df_emp.get("department_name", []).tolist() if n})
            team_pairs = sorted({
                (r["department_name"], r["team_name"])
                for _, r in df_emp[["department_name", "team_name"]].iterrows()
                if str(r.get("team_name", "")).strip()
            })

            # department を name で upsert
            for dn in dep_names:
                dep_id = con.execute(text(
                    "SELECT department_id FROM department WHERE department_name=:dn"
                ), {"dn": dn}).scalar_one_or_none()
                if dep_id is None:
                    con.execute(text(
                        "INSERT INTO department (department_name) VALUES (:dn)"
                    ), {"dn": dn})

            # team を (team_name, department) で upsert
            for dn, tn in team_pairs:
                dep_id = None
                if dn:
                    dep_id = con.execute(text(
                        "SELECT department_id FROM department WHERE department_name=:dn"
                    ), {"dn": dn}).scalar_one_or_none()
                    if dep_id is None:
                        con.execute(text(
                            "INSERT INTO department (department_name) VALUES (:dn)"
                        ), {"dn": dn})
                        dep_id = con.execute(text(
                            "SELECT department_id FROM department WHERE department_name=:dn"
                        ), {"dn": dn}).scalar_one()
                team_id = con.execute(text(
                    "SELECT team_id FROM team WHERE team_name=:tn"
                ), {"tn": tn}).scalar_one_or_none()
                if team_id is None:
                    con.execute(text(
                        "INSERT INTO team (team_name, department_id) VALUES (:tn, :did)"
                    ), {"tn": tn, "did": dep_id})

            # 最新の team マップ
            teams = pd.read_sql(text("SELECT team_id, team_name FROM team"), con)
            tmap = teams.set_index("team_name")["team_id"].to_dict()

            # 2) ブール正規化 / 数値化
            def to_bool(x):
                s = str(x).strip()
                return s in ("True", "TRUE", "true", "1", "○", "◯", "Yes", "YES", "はい")

            for c in ["is_leader", "is_vice_leader", "is_certifier"]:
                if c in df_emp.columns:
                    df_emp[c] = df_emp[c].apply(to_bool)

            for c in ["default_work_hours", "monthly_work_hours"]:
                if c in df_emp.columns:
                    df_emp[c] = pd.to_numeric(df_emp[c], errors="coerce")

            # 3) employee を UPSERT
            for _, r in df_emp.iterrows():
                code = str(r.get("employee_code") or "").strip()
                if not code:
                    continue
                team_id = None
                if "team_name" in r and str(r["team_name"]).strip():
                    team_id = tmap.get(str(r["team_name"]).strip())

                payload = {
                    "code": code,
                    "name": r.get("name"),
                    "employment_type": r.get("employment_type"),
                    "position": r.get("position"),
                    "is_leader": bool(r.get("is_leader", False)),
                    "is_vice_leader": bool(r.get("is_vice_leader", False)),
                    "is_certifier": bool(r.get("is_certifier", False)),
                    "dwh": None if pd.isna(r.get("default_work_hours")) else float(r.get("default_work_hours")),
                    "mwh": None if pd.isna(r.get("monthly_work_hours")) else float(r.get("monthly_work_hours")),
                    "team_id": team_id,
                }

                con.execute(text("""
                    INSERT INTO employee (
                        employee_code, name, employment_type, position,
                        is_leader, is_vice_leader, is_certifier,
                        default_work_hours, monthly_work_hours,
                        team_id, updated_at
                    )
                    VALUES (
                        :code, :name, :employment_type, :position,
                        :is_leader, :is_vice_leader, :is_certifier,
                        :dwh, :mwh, :team_id, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (employee_code) DO UPDATE SET
                      name               = EXCLUDED.name,
                      employment_type    = EXCLUDED.employment_type,
                      position           = EXCLUDED.position,
                      is_leader          = EXCLUDED.is_leader,
                      is_vice_leader     = EXCLUDED.is_vice_leader,
                      is_certifier       = EXCLUDED.is_certifier,
                      default_work_hours = EXCLUDED.default_work_hours,
                      monthly_work_hours = EXCLUDED.monthly_work_hours,
                      team_id            = COALESCE(EXCLUDED.team_id, employee.team_id),
                      updated_at         = CURRENT_TIMESTAMP
                """), payload)

        # ---------- B) 区情報（zone を補完 → demandprofile） ----------
        if "区情報" in xls.sheet_names:
            dfz = pd.read_excel(xls, sheet_name="区情報").rename(columns={
                "区コード": "zone_code",
                "区名": "zone_name",
                "班": "team_name",
                "稼働": "operational_status",
                "月": "mon", "火": "tue", "水": "wed", "木": "thu",
                "金": "fri", "土": "sat", "日": "sun", "祝": "holiday",
            })
            # 前処理
            for c in ["team_name", "zone_code", "zone_name", "operational_status"]:
                if c in dfz.columns:
                    dfz[c] = dfz[c].fillna("").astype(str).str.strip()

            # team 補完
            for tn in sorted({t for t in dfz.get("team_name", []).tolist() if t}):
                tid = con.execute(text("SELECT team_id FROM team WHERE team_name=:tn"),
                                  {"tn": tn}).scalar_one_or_none()
                if tid is None:
                    con.execute(text("INSERT INTO team (team_name) VALUES (:tn)"), {"tn": tn})

            # zone 補完（zone_code が一意ならそれで、無ければ (team_id, zone_name) で近似）
            for _, r in dfz.iterrows():
                tn = r.get("team_name"); zc = r.get("zone_code"); zn = r.get("zone_name"); op = r.get("operational_status")
                if not tn:
                    continue
                tid = con.execute(text("SELECT team_id FROM team WHERE team_name=:tn"), {"tn": tn}).scalar_one_or_none()
                if not tid:
                    continue
                if zc:
                    zid = con.execute(text("SELECT zone_id FROM zone WHERE zone_code=:zc"),
                                      {"zc": zc}).scalar_one_or_none()
                    if zid is None:
                        con.execute(text("""
                            INSERT INTO zone (team_id, zone_code, zone_name, operational_status)
                            VALUES (:tid,:zc,:zn,:op)
                        """), {"tid": tid, "zc": zc, "zn": zn, "op": op})
                else:
                    zid = con.execute(text("""
                        SELECT z.zone_id FROM zone z
                         JOIN team t ON t.team_id = z.team_id
                        WHERE t.team_name = :tn AND z.zone_name = :zn
                    """), {"tn": tn, "zn": zn}).scalar_one_or_none()
                    if zid is None:
                        con.execute(text("""
                            INSERT INTO zone (team_id, zone_name, operational_status)
                            VALUES (:tid,:zn,:op)
                        """), {"tid": tid, "zn": zn, "op": op})

            # 最新の zone マップ（code優先）
            zones2 = pd.read_sql(text("SELECT zone_id, zone_code, zone_name, team_id FROM zone"), con)
            zmap_code = zones2.set_index("zone_code")["zone_id"].dropna().to_dict()
            zones2["tn"] = zones2["team_id"].astype(str) + "||" + zones2["zone_name"].fillna("")
            zmap_name = zones2.set_index("tn")["zone_id"].to_dict()

            # demandprofile を upsert
            for _, r in dfz.iterrows():
                tn = str(r.get("team_name") or "").strip()
                zc = str(r.get("zone_code") or "").strip()
                zn = str(r.get("zone_name") or "").strip()

                zid = None
                if zc:
                    zid = zmap_code.get(zc)
                if zid is None and tn and zn:
                    tid = con.execute(text("SELECT team_id FROM team WHERE team_name=:tn"),
                                      {"tn": tn}).scalar_one_or_none()
                    if tid:
                        key = f"{tid}||{zn}"
                        zid = zmap_name.get(key)
                if not zid:
                    continue  # zone 未作成ならスキップ

                payload = {
                    "z": int(zid),
                    "mon": int(r.get("mon", 0) or 0),
                    "tue": int(r.get("tue", 0) or 0),
                    "wed": int(r.get("wed", 0) or 0),
                    "thu": int(r.get("thu", 0) or 0),
                    "fri": int(r.get("fri", 0) or 0),
                    "sat": int(r.get("sat", 0) or 0),
                    "sun": int(r.get("sun", 0) or 0),
                    "hol": int(r.get("holiday", 0) or 0),
                }
                con.execute(text("""
                    INSERT INTO demandprofile
                      (zone_id, demand_mon, demand_tue, demand_wed, demand_thu,
                       demand_fri, demand_sat, demand_sun, demand_holiday)
                    VALUES (:z,:mon,:tue,:wed,:thu,:fri,:sat,:sun,:hol)
                    ON CONFLICT (zone_id) DO UPDATE SET
                      demand_mon=EXCLUDED.demand_mon,
                      demand_tue=EXCLUDED.demand_tue,
                      demand_wed=EXCLUDED.demand_wed,
                      demand_thu=EXCLUDED.demand_thu,
                      demand_fri=EXCLUDED.demand_fri,
                      demand_sat=EXCLUDED.demand_sat,
                      demand_sun=EXCLUDED.demand_sun,
                      demand_holiday=EXCLUDED.demand_holiday
                """), payload)

        # ---------- A) 社員別需要（availability / proficiency） ----------
        if "社員別需要" in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name="社員別需要").rename(columns={
                "社員番号": "employee_code",
                "氏名": "name",
                "月": "mon", "火": "tue", "水": "wed", "木": "thu",
                "金": "fri", "土": "sat", "日": "sun", "祝": "hol"
            })
            fixed = {"employee_code","name","mon","tue","wed","thu","fri","sat","sun","hol"}
            zone_name_cols = [c for c in df.columns if c not in fixed]

            # employee map
            emp = pd.read_sql(text("SELECT employee_id, employee_code FROM employee"), con)
            emap = emp.set_index("employee_code")["employee_id"].astype(int).to_dict()

            # zone map（名前→id・班を考慮）
            zones = pd.read_sql(text("""
                SELECT z.zone_id, z.zone_name, t.team_name
                  FROM zone z JOIN team t ON z.team_id=t.team_id
            """), con)
            zmap = zones.set_index(["team_name", "zone_name"])["zone_id"].astype(int).to_dict()

            # availability
            if all(c in df.columns for c in ["mon","tue","wed","thu","fri","sat","sun","hol"]):
                for _, r in df.iterrows():
                    eid = emap.get(str(r.get("employee_code")))
                    if eid is None:
                        continue
                    vals = {c: bool(r.get(c)) for c in ["mon","tue","wed","thu","fri","sat","sun","hol"]}
                    con.execute(text("""
                        INSERT INTO employeeavailability
                          (employee_id, available_mon, available_tue, available_wed, available_thu,
                           available_fri, available_sat, available_sun, available_hol)
                        VALUES (:eid,:mon,:tue,:wed,:thu,:fri,:sat,:sun,:hol)
                        ON CONFLICT (employee_id) DO UPDATE SET
                          available_mon=EXCLUDED.available_mon,
                          available_tue=EXCLUDED.available_tue,
                          available_wed=EXCLUDED.available_wed,
                          available_thu=EXCLUDED.available_thu,
                          available_fri=EXCLUDED.available_fri,
                          available_sat=EXCLUDED.available_sat,
                          available_sun=EXCLUDED.available_sun,
                          available_hol=EXCLUDED.available_hol
                    """), {"eid": int(eid), **vals})

            # proficiency（区名ごと）
            for _, r in df.iterrows():
                eid = emap.get(str(r.get("employee_code")))
                if eid is None:
                    continue
                for zn in zone_name_cols:
                    v = r.get(zn)
                    if pd.isna(v):
                        continue
                    zid = zmap.get((team_name, str(zn))) if team_name else None
                    if zid is None:
                        continue
                    con.execute(text("""
                        INSERT INTO employeezoneproficiency (employee_id, zone_id, proficiency)
                        VALUES (:eid,:zid,:p)
                        ON CONFLICT (employee_id, zone_id) DO UPDATE SET proficiency=EXCLUDED.proficiency
                    """), {"eid": int(eid), "zid": int(zid), "p": int(v)})

    typer.echo(f"✅ Imported to DB from: {file}")

if __name__ == "__main__":
    app()
