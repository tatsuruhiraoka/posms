# posms/cli.py
"""
posms.cli
=========

Command-line interface for the Postal Operation Shift-Management System.

Usage examples
--------------

# 予測 + シフト最適化 (E2E)
poetry run posms run-monthly --predict-date 2025-08-01

# 予測モデルだけ再学習（normal + registered_plus）
poetry run posms train --office-id 1

# 指定開始日から4週間の予測をDBへ書き戻し（mail_kind 指定）
poetry run posms forecast --office-id 1 --start 2025-09-08 --days 28 --mail-kind normal
poetry run posms forecast --office-id 1 --start 2025-09-08 --days 28 --mail-kind registered_plus

# シフトだけ再最適化（分担表案）
poetry run posms optimize --date 2025-08-01 --output-type 分担表案

# 初期マスタ投入（任意：PostgreSQL環境の初期化に便利）
poetry run posms seed-masters
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from enum import Enum
from typing import Annotated

import os
import re
from shutil import copyfile

import mlflow
import numpy as np
import pandas as pd
import jpholiday
import typer
from sqlalchemy import inspect, text

from .exporters.excel_exporter import write_dataframe_to_excel
from posms.utils.db import SessionManager

app = typer.Typer(help="Postal Operation Shift-Management System CLI")

# 既存の try/except ～ ダミー定義を丸ごと置き換え
try:
    from posms.optimization.shift_builder import OutputType  # 本物（あれば）
except Exception:

    class OutputType(str, Enum):  # Typer が扱える
        分担表 = "分担表"
        勤務指定表 = "勤務指定表"
        分担表案 = "分担表案"


# ---------- Helper -------------------------------------------------
def _default_template() -> Path:
    return Path("excel_templates/shift_template.xlsx")


def _engine():
    """SessionManager 経由で Engine を取得（Postgres⇄SQLite 自動切替）"""
    return SessionManager().engine


def _existing_tables_lower(eng) -> set[str]:
    """接続先DBのテーブル名一覧（小文字）を返す。PostgreSQLはschema='public'を考慮。"""
    insp = inspect(eng)
    try:
        if eng.dialect.name == "postgresql":
            names = insp.get_table_names(schema="public")
        else:
            names = insp.get_table_names()
    except Exception:
        try:
            names = insp.get_table_names()
        except Exception:
            names = []
    return {str(t).lower() for t in names}


def _has_table(eng, name: str) -> bool:
    """方言差を吸収してテーブル存在を確認"""
    insp = inspect(eng)
    schema = "public" if eng.dialect.name == "postgresql" else None
    try:
        return insp.has_table(name, schema=schema)
    except TypeError:
        return insp.has_table(name)


def _strip_quotes(name: str) -> str:
    s = str(name).strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return s[1:-1]
    return s


def _has_column(eng, table_name: str, column_name: str) -> bool:
    """テーブルのカラム有無を方言差も含めて確認（ケース差も吸収）"""
    insp = inspect(eng)
    schema = "public" if eng.dialect.name == "postgresql" else None
    t = _strip_quotes(table_name)

    try:
        cols = insp.get_columns(t, schema=schema)
    except TypeError:
        cols = insp.get_columns(t)
    except Exception:
        try:
            cols = insp.get_columns(t)
        except Exception:
            cols = []

    names = set()
    for c in cols:
        if isinstance(c, dict) and "name" in c:
            names.add(str(c["name"]).lower())
        else:
            try:
                names.add(str(c.name).lower())
            except Exception:
                pass
    return column_name.lower() in names


def _resolve_mailvolume_table(eng) -> str:
    existing = _existing_tables_lower(eng)
    if "mailvolume" in existing:
        return "mailvolume"
    if "mail_volume" in existing:
        return "mail_volume"
    return '"MailVolume"'  # 引用付きで作成された場合のフォールバック


def _resolve_jobtype_table(eng) -> str:
    existing = _existing_tables_lower(eng)
    if "jobtype" in existing:
        return "jobtype"
    if "job_type" in existing:
        return "job_type"
    return '"JobType"'  # 大文字作成や引用付テーブルに対応


def _season(ts: pd.Timestamp) -> int:
    """1:春(3-5), 2:夏(6-8), 3:秋(9-11), 4:冬(12-2)"""
    m = ts.month
    return (
        1 if m in (3, 4, 5) else 2 if m in (6, 7, 8) else 3 if m in (9, 10, 11) else 4
    )


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


def _truthy_int(x) -> int:
    """'○', '◯', 'true', '1', 'yes' などを 1、'×', 'false', '0', 'no' を 0 に正規化（値は厳格化対象外）"""
    s = str(x).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "on", "○", "◯"):
        return 1
    if s in ("0", "false", "f", "no", "n", "off", "×"):
        return 0
    try:
        return 1 if int(float(s)) != 0 else 0
    except Exception:
        return 0


def _extract_first_int(x: object, default: int = 10**9) -> int:
    m = re.search(r"(\d+)", str(x))
    return int(m.group(1)) if m else default


def _sort_by_display_order_or_natural(
    df: pd.DataFrame,
    code_col: str,
    display_col: str = "__display_order__",
) -> pd.DataFrame:
    """
    display_order が有効ならそれで昇順。
    無ければ code_col（例: 'Z10'）を自然順（数値）で昇順。
    """
    out = df.copy()

    if (
        display_col in out.columns
        and pd.to_numeric(out[display_col], errors="coerce").notna().any()
    ):
        out["__ord__"] = (
            pd.to_numeric(out[display_col], errors="coerce").fillna(10**9).astype(int)
        )
        sort_cols = ["__ord__"]
        if code_col in out.columns:
            sort_cols.append(code_col)
        out = out.sort_values(sort_cols, kind="mergesort")
        out = out.drop(columns=["__ord__"])
        return out

    if code_col in out.columns:
        out["__ord__"] = out[code_col].astype(str).map(_extract_first_int).astype(int)
        out = out.sort_values(["__ord__", code_col], kind="mergesort").drop(
            columns=["__ord__"]
        )
        return out

    return out


def _write_df_overwrite_sheet(
    df: pd.DataFrame,
    out_path: Path,
    sheet_name: str,
    template_path: Path | None = None,
    start_cell: str = "A1",
) -> None:
    """
    Excelに「二重出力」させないための上書き書き込み。

    - template_path があればそれを開く（無ければ out_path が存在すれば out を開く）
    - sheet_name の start_cell から df を書く
    - 既存データ領域は value を None にしてクリアしてから上書き（スタイルは極力残る）
    - xlsm は keep_vba=True でマクロ保持
    """
    import openpyxl
    from openpyxl.utils.cell import coordinate_to_tuple

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tpl = Path(template_path) if template_path else None

    load_path: Path | None = None
    if tpl is not None and tpl.exists():
        load_path = tpl
    elif out_path.exists():
        load_path = out_path

    if load_path is None:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = sheet_name
    else:
        wb = openpyxl.load_workbook(
            load_path, keep_vba=(load_path.suffix.lower() == ".xlsm")
        )
        if sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
        else:
            ws = wb.create_sheet(sheet_name)

    start_row, start_col = coordinate_to_tuple(start_cell)

    # クリア範囲（既存の最大 or 新規の必要範囲）
    nrows = int(len(df.index))
    ncols = int(len(df.columns))
    need_last_row = start_row + 1 + max(0, nrows)  # header + data
    need_last_col = start_col + max(0, ncols - 1)

    clear_last_row = max(ws.max_row, need_last_row)
    clear_last_col = max(ws.max_column, need_last_col)

    # 値クリア（スタイルは残す）
    for r in range(start_row, clear_last_row + 1):
        for c in range(start_col, clear_last_col + 1):
            ws.cell(row=r, column=c).value = None

    # ヘッダ
    for j, col_name in enumerate(df.columns, start=start_col):
        ws.cell(row=start_row, column=j).value = str(col_name)

    # データ
    # NaN/NaT → None
    values = df.to_numpy()
    for i, row_vals in enumerate(values, start=start_row + 1):
        for j, v in enumerate(row_vals, start=start_col):
            if pd.isna(v):
                v = None
            ws.cell(row=i, column=j).value = v

    wb.save(out_path)
    wb.close()


# ---------- Commands ----------------------------------------------
@app.command("run-monthly")
def run_monthly(
    predict_date: Annotated[
        str,
        typer.Option("--predict-date", "-d", help="YYYY-MM-DD 形式。省略時は今日。"),
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
        Path, typer.Option("--template", "-t", help="Excel テンプレート .xlsx")
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
    # refresh or train（モジュール側で分岐）
    try:
        from posms.flows.monthly_flow import monthly_refresh as _monthly
    except ImportError:
        from posms.flows.monthly_flow import monthly_train as _monthly
    _monthly(**params)


@app.command("train")
def train_model(
    office_id: Annotated[
        int | None, typer.Option("--office-id", help="局ID（1局のみなら省略可）")
    ] = None,
    n_estimators: Annotated[int, typer.Option(help="XGBoost 木数")] = 200,
    max_depth: Annotated[int, typer.Option(help="XGBoost 木の深さ")] = 6,
):
    """DB を使ってモデルを学習し、MLflowに保存。normal + registered_plus を両方実行。"""
    # 遅延 import（export系の import 連鎖を防ぐ）
    from posms.features.builder import FeatureBuilder
    from posms.models.normal.trainer import ModelTrainer as NormalTrainer
    from posms.models.registered_plus.trainer import ModelTrainer as RegPlusTrainer

    params = {"n_estimators": n_estimators, "max_depth": max_depth}

    # normal
    fb_n = FeatureBuilder(office_id=office_id, mail_kind="normal")
    Xn, yn = fb_n.build()
    run_id_normal = NormalTrainer(params).train(Xn, yn)

    # registered_plus（同じ Engine を共有）
    fb_r = FeatureBuilder(
        office_id=office_id, mail_kind="registered_plus", engine=fb_n.engine
    )
    Xr, yr = fb_r.build()
    run_id_regplus = RegPlusTrainer(params).train(Xr, yr)

    typer.echo(f"MLflow run_id normal: {run_id_normal}")
    typer.echo(f"MLflow run_id registered_plus: {run_id_regplus}")


@app.command("forecast")
def forecast_4weeks(
    start: Annotated[str, typer.Option("--start", "-s", help="予測開始日 YYYY-MM-DD")],
    days: Annotated[int, typer.Option("--days", "-n", help="予測日数（既定=28）")] = 28,
    office_id: Annotated[
        int | None, typer.Option("--office-id", help="局ID（1局のみなら省略可）")
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option(
            "--run-id", help="使用する学習 run_id。未指定なら Production/最新run"
        ),
    ] = None,
    stage: Annotated[
        str, typer.Option("--stage", help="Model Registry ステージ名")
    ] = "Production",
    mail_kind: Annotated[
        str, typer.Option("--mail-kind", help="normal / registered_plus")
    ] = "normal",
):
    """
    学習済みモデルで、指定開始日から days 日分のローリング予測を行い、
    MailVolume.forecast_volume を更新します。
    """
    # 遅延 import（export系の import 連鎖を防ぐ）
    from posms.features.builder import FEATURE_COLUMNS, FeatureBuilder

    if mail_kind not in ("normal", "registered_plus"):
        raise typer.BadParameter(f"Unknown mail_kind: {mail_kind}")

    # 特徴量（DBから）
    fb = FeatureBuilder(office_id=office_id, mail_kind=mail_kind)
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
                    rows = con.execute(
                        _t(f"SELECT DISTINCT office_id FROM {MV} ORDER BY office_id")
                    ).fetchall()
                if len(rows) == 1:
                    office_id = int(rows[0][0])
                elif len(rows) == 0:
                    typer.secho(
                        "MailVolume にレコードがありません。先に学習用データを投入してください。",
                        err=True,
                    )
                    raise typer.Exit(code=1)
                else:
                    ids = ", ".join(str(r[0]) for r in rows)
                    typer.secho(
                        f"複数の office_id が見つかりました: [{ids}]  --office-id を指定してください。",
                        err=True,
                    )
                    raise typer.Exit(code=2)
        except Exception as e:
            typer.secho(f"office_id 自動解決に失敗: {e}", err=True)
            raise typer.Exit(code=3)

    # 期間の行が無ければ作る（actual=NULL, forecast=NULL, price_increase_flagは既存値/無ければ0）
    start_ts = pd.to_datetime(start).normalize()
    targets = pd.date_range(start_ts, periods=days, freq="D")

    with eng.begin() as con:
        df_flags = pd.read_sql(
            text(
                f'SELECT "date", price_increase_flag FROM {MV} WHERE office_id=:o AND "date" BETWEEN :d1 AND :d2'
            ),
            con,
            params={"o": office_id, "d1": targets[0], "d2": targets[-1]},
            parse_dates=["date"],
        ).set_index("date")
        for dt in targets:
            val = (
                df_flags["price_increase_flag"].get(dt) if not df_flags.empty else None
            )
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
                    f"VALUES (:d, :o, NULL, NULL, :f) "
                    f'ON CONFLICT ("date", office_id) DO NOTHING'
                ),
                {"d": dt.date(), "o": int(office_id), "f": flag},
            )

    # 既存の系列（actual）で進め、未来は予測で埋めながらローリング
    hist = fb._load_mail().set_index("date").sort_index()
    vol = hist["actual_volume"].astype("float").copy()

    updates: list[tuple[date, int]] = []
    for ts in targets:
        prev_1 = ts - pd.Timedelta(days=1)
        prev_7 = ts - pd.Timedelta(days=7)
        if prev_1 not in vol.index or prev_7 not in vol.index:
            continue
        last7 = vol.loc[ts - pd.Timedelta(days=7) : ts - pd.Timedelta(days=1)]
        if len(last7) < 7 or last7.isna().any():
            continue

        flag = (
            int(hist["price_increase_flag"].get(ts, 0))
            if "price_increase_flag" in hist.columns
            else 0
        )
        _ = pd.DataFrame(
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

        # 予測器は FeatureBuilder 側（mail_kind別）で解決
        yhat_raw = float(
            fb.predict(
                target_date=ts.date(),
                run_id=run_id,
                stage=stage,
                model_name="posms",
                tracking_uri=None,
            )
        )

        # 負の予測は 0 にクリップ（count系のため）
        yhat = max(0.0, yhat_raw)
        vol.loc[ts] = yhat  # 次日の特徴量計算のために予測値を採用
        updates.append((ts.date(), int(round(yhat))))

    # 書き戻し
    if updates:
        with eng.begin() as con:
            for d, v in updates:
                con.execute(
                    text(
                        f'UPDATE {MV} SET forecast_volume=:v, updated_at=NOW() WHERE "date"=:d AND office_id=:o'
                    ),
                    {"v": v, "d": d, "o": int(office_id)},
                )
        typer.echo(
            f"forecast_volume 更新: {len(updates)} 件（mail_kind={mail_kind}, office_id={office_id}, {targets[0].date()}〜{targets[-1].date()}）"
        )
    else:
        typer.echo("更新対象なし（直近実績不足 or 行未作成）")

    typer.echo(f"MLflow: {mlflow.get_tracking_uri()}")


@app.command("optimize")
def optimize_shift(
    date_str: Annotated[str, typer.Option("--date", "-d")] = str(date.today()),
    output_type: Annotated[
        OutputType,
        typer.Option(help="分担表 / 勤務指定表 / 分担表案", case_sensitive=False),
    ] = OutputType.分担表,
    template: Annotated[
        Path, typer.Option("--template", "-t", help="Excel テンプレート")
    ] = _default_template(),
):
    from posms.optimization.shift_builder import ShiftBuilder
    from posms.features.builder import FeatureBuilder

    """需要予測済みデータを入力にシフトのみ再最適化"""
    demand = FeatureBuilder(mail_kind="normal").predict(
        date_str
    )  # 既定 run_id を内部でロード
    staff = FeatureBuilder(mail_kind="normal").load_staff()
    out = ShiftBuilder(template).build(demand, staff, output_type)
    typer.echo(f"Excel saved → {out.resolve()}")


@app.command("export-excel")
def export_excel(
    sql: str = typer.Option(
        None, "--sql", help="実行するSQL。--query-fileとどちらか必須"
    ),
    query_file: Path = typer.Option(
        None, "--query-file", exists=True, help="SQLファイルパス。--sqlとどちらか必須"
    ),
    out: Path = typer.Option(
        Path("dist/shift_report.xlsx"), "--out", help="出力先 .xlsx"
    ),
    sheet: str = typer.Option("export", "--sheet", help="書き込み先シート名"),
    template: Path = typer.Option(
        None, "--template", help="テンプレ .xlsx（未指定なら空ブック）"
    ),
    start_cell: str = typer.Option("A1", "--start-cell", help="開始セル（例:A1）"),
    header_map: str | None = typer.Option(
        None,
        "--header-map",
        help="英=日 をカンマ区切りで指定。例: employee_code=社員コード,name=氏名,team_name=班",
    ),
    append: bool = typer.Option(False, "--append", help="既存の .xlsx にシートを追記"),
    sort_by: str | None = typer.Option(
        None, "--sort-by", help="カンマ区切りの列名で昇順ソート"
    ),
    sort_natural: str | None = typer.Option(
        None, "--sort-natural", help="自然順ソートする列名（例: 社員番号, zone_code）"
    ),
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
        col = sort_natural.strip()
        if col in df.columns:
            key = (
                df[col]
                .astype(str)
                .str.extract(r"(\d+)", expand=False)
                .fillna("0")
                .astype(int)
            )
            df = (
                df.assign(__key__=key)
                .sort_values(["__key__", col])
                .drop(columns="__key__")
            )

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
    department_code: str = typer.Option(
        ..., "--department-code", "-dc", help="例: DPT-A もしくは 部署名"
    ),
    team: str = typer.Option(..., "--team", "-t", help="例: 1班"),
    out: Path = typer.Option(Path("excel_out/班データ.xlsx"), "--out"),
    sheet: str = typer.Option("社員", "--sheet"),
    template: Path = typer.Option(None, "--template"),
):
    eng = _engine()

    insp = inspect(eng)
    dept_cols = {
        c["name"] if isinstance(c, dict) else c.name
        for c in insp.get_columns("department")
    }
    has_code = "department_code" in dept_cols

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

    # 自然順ソート：社員番号の数字部分で並び替え
    if "社員番号" in df.columns:
        df["__num__"] = (
            df["社員番号"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("999999999")
            .astype(int)
        )
        df = df.sort_values(["__num__", "社員番号"], kind="mergesort").drop(
            columns="__num__"
        )

    for c in ["班長", "副班長", "認証司", "勤務時間(日)", "勤務時間(月)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    _write_df_overwrite_sheet(
        df=df,
        out_path=out,
        sheet_name=sheet,
        template_path=template,
        start_cell="A1",
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
    dept_cols = {
        c["name"] if isinstance(c, dict) else c.name
        for c in insp.get_columns("department")
    }
    has_code = "department_code" in dept_cols

    has_zone_order = _has_column(eng, "zone", "display_order")
    order_select = (
        'z.display_order AS "__display_order__"'
        if has_zone_order
        else 'NULL AS "__display_order__"'
    )

    base_sql = f"""
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
        COALESCE(dem.demand_holiday,0) AS "祝",
        COALESCE(z.shift_type,'日勤')   AS "シフトタイプ",
        {order_select}
      FROM zone z
      JOIN team t          ON t.team_id = z.team_id
      JOIN department d    ON d.department_id = t.department_id
      LEFT JOIN demandprofile dem ON dem.zone_id = z.zone_id
      WHERE {{where_clause}}
    """

    if has_code:
        where_clause = "(COALESCE(d.department_code,'') = :dc OR d.department_name = :dc) AND t.team_name = :tn"
    else:
        where_clause = "d.department_name = :dc AND t.team_name = :tn"

    sql = base_sql.format(where_clause=where_clause)

    with eng.connect() as con:
        df = pd.read_sql(text(sql), con, params={"dc": department_code, "tn": team})

    # display_order優先、無ければ区コード自然順
    df = _sort_by_display_order_or_natural(
        df, code_col="区コード", display_col="__display_order__"
    )
    if "__display_order__" in df.columns:
        df = df.drop(columns=["__display_order__"])

    _write_df_overwrite_sheet(
        df=df,
        out_path=out,
        sheet_name=sheet,
        template_path=template,
        start_cell="A1",
    )
    typer.echo(f"✅ 区情報シート {len(df)}行 → {out}（{sheet}）")


@app.command("export-sheet-employee-demand")
def export_sheet_employee_demand(
    team: str = typer.Option(..., "--team", "-t", help="例: 1班"),
    out: Path = typer.Option(Path("excel_out/班データ.xlsx"), "--out"),
    template: Path = typer.Option(
        None, "--template", help="xls/xlsx/xlsm（xlsmはマクロ温存）"
    ),
    sheet: str = typer.Option("社員別需要", "--sheet"),
):
    """
    班ごとの『社員別需要』シートをExcelに出力:
      列 = 社員番号, 氏名, <区名...横展開>, 月, 火, 水, 木, 金, 土, 日, 祝
      値 = 区列: EmployeeZoneProficiency.proficiency（無い所は0） / 曜日列: 1/0
    """
    eng = _engine()

    has_zone_order = _has_column(eng, "zone", "display_order")
    zone_order_select = (
        "z.display_order AS display_order"
        if has_zone_order
        else "NULL AS display_order"
    )

    sql_zones = f"""
      SELECT z.zone_id, z.zone_code, z.zone_name, {zone_order_select}
        FROM zone z
        JOIN team t ON z.team_id = t.team_id
       WHERE t.team_name = :team
    """

    sql_emp = """
      SELECT e.employee_id, e.employee_code, e.name,
             CASE WHEN COALESCE(ea.available_mon,  1) <> 0 THEN 1 ELSE 0 END AS mon,
             CASE WHEN COALESCE(ea.available_tue,  1) <> 0 THEN 1 ELSE 0 END AS tue,
             CASE WHEN COALESCE(ea.available_wed,  1) <> 0 THEN 1 ELSE 0 END AS wed,
             CASE WHEN COALESCE(ea.available_thu,  1) <> 0 THEN 1 ELSE 0 END AS thu,
             CASE WHEN COALESCE(ea.available_fri,  1) <> 0 THEN 1 ELSE 0 END AS fri,
             CASE WHEN COALESCE(ea.available_sat,  0) <> 0 THEN 1 ELSE 0 END AS sat,
             CASE WHEN COALESCE(ea.available_sun,  0) <> 0 THEN 1 ELSE 0 END AS sun,
             CASE WHEN COALESCE(ea.available_hol,  0) <> 0 THEN 1 ELSE 0 END AS hol,
             COALESCE(ea.available_early,     '') AS early,
             COALESCE(ea.available_day,       '') AS day,
             COALESCE(ea.available_mid,       '') AS mid,
             COALESCE(ea.available_night,     '') AS night,
             COALESCE(ea.available_night_sat, '') AS night_sat,
             COALESCE(ea.available_night_sun, '') AS night_sun,
             COALESCE(ea.available_night_hol, '') AS night_hol
        FROM employee e
        JOIN team t ON e.team_id = t.team_id
   LEFT JOIN employee_availabilities ea ON ea.employee_id = e.employee_id
       WHERE t.team_name = :team
    """

    sql_prof = """
      SELECT ezp.employee_id, z.zone_name, COALESCE(ezp.proficiency, 0) AS proficiency
        FROM employeezoneproficiency ezp
        JOIN zone z ON z.zone_id = ezp.zone_id
        JOIN team t ON z.team_id = t.team_id
       WHERE t.team_name = :team
    """

    with eng.connect() as con:
        df_z = pd.read_sql(text(sql_zones), con, params={"team": team})
        df_e = pd.read_sql(text(sql_emp), con, params={"team": team})
        df_p = pd.read_sql(text(sql_prof), con, params={"team": team})

    # 列名小文字化
    for dfx in (df_z, df_e, df_p):
        dfx.columns = [c.lower() for c in dfx.columns]

    # 区の並びを display_order優先→無ければ zone_code自然順
    if (
        "display_order" in df_z.columns
        and pd.to_numeric(df_z["display_order"], errors="coerce").notna().any()
    ):
        df_z["__ord__"] = (
            pd.to_numeric(df_z["display_order"], errors="coerce")
            .fillna(10**9)
            .astype(int)
        )
        df_z = df_z.sort_values(["__ord__", "zone_code"], kind="mergesort").drop(
            columns=["__ord__"]
        )
    else:
        if "zone_code" in df_z.columns:
            df_z["__ord__"] = (
                df_z["zone_code"].astype(str).map(_extract_first_int).astype(int)
            )
            df_z = df_z.sort_values(["__ord__", "zone_code"], kind="mergesort").drop(
                columns=["__ord__"]
            )
        else:
            df_z = df_z.sort_values(["zone_id"], kind="mergesort")

    # 社員コードの自然順
    if "employee_code" in df_e.columns:
        df_e["__num__"] = (
            df_e["employee_code"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("999999999")
            .astype(int)
        )
        df_e = df_e.sort_values(["__num__", "employee_code"], kind="mergesort").drop(
            columns="__num__"
        )

    zone_cols = df_z["zone_name"].dropna().astype(str).tolist()

    # 熟練度を横展開
    if df_p.empty:
        wide = pd.DataFrame(columns=["employee_id"] + zone_cols)
    else:
        wide = (
            df_p.pivot_table(
                index="employee_id",
                columns="zone_name",
                values="proficiency",
                fill_value=0,
                aggfunc="max",
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )

    df = df_e.merge(wide, on="employee_id", how="left")

    for zc in zone_cols:
        if zc not in df.columns:
            df[zc] = 0
        df[zc] = pd.to_numeric(df[zc], errors="coerce").fillna(0).astype("int64")

    for c in ["mon", "tue", "wed", "thu", "fri", "sat", "sun", "hol"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    ordered = (
        ["employee_code", "name"]
        + zone_cols
        + [
            "mon",
            "tue",
            "wed",
            "thu",
            "fri",
            "sat",
            "sun",
            "hol",
            "early",
            "day",
            "mid",
            "night",
            "night_sat",
            "night_sun",
            "night_hol",
        ]
    )
    ordered = [c for c in ordered if c in df.columns]
    df = df[ordered].rename(
        columns={
            "employee_code": "社員番号",
            "name": "氏名",
            "mon": "月",
            "tue": "火",
            "wed": "水",
            "thu": "木",
            "fri": "金",
            "sat": "土",
            "sun": "日",
            "hol": "祝",
            "early": "早番",
            "day": "日勤",
            "mid": "中勤",
            "night": "夜勤",
            "night_sat": "夜勤(土)",
            "night_sun": "夜勤(日)",
            "night_hol": "夜勤(祝)",
        }
    )

    _write_df_overwrite_sheet(
        df=df,
        out_path=out,
        sheet_name=sheet,
        template_path=template,
        start_cell="A1",
    )
    typer.echo(f"✅ 社員別需要シート {len(df)}行 → {out}（{sheet}）")


@app.command("export-sheet-jobtypes-regular")
def export_sheet_jobtypes_regular(
    out: Path = typer.Option(Path("excel_out/班データ統合.xlsx"), "--out"),
    template: Path = typer.Option(None, "--template"),
    sheet: str = typer.Option("勤務種別(正社員)", "--sheet"),
):
    eng = _engine()
    JT = _resolve_jobtype_table(eng)
    jt_for_inspect = _strip_quotes(JT)

    has_disp = _has_column(eng, jt_for_inspect, "display_order")
    disp_select = (
        'display_order AS "__display_order__",'
        if has_disp
        else 'NULL AS "__display_order__",'
    )

    sql = f"""
      SELECT
             {disp_select}
             job_name   AS "勤務名",
             start_time AS "就労開始時間",
             end_time   AS "就労終了時間",
             work_hours AS "勤務時間"
        FROM {JT}
       WHERE lower(COALESCE(classification,'')) IN ('reg','regular','fulltime','正社員')
    """

    with eng.connect() as con:
        df = pd.read_sql(text(sql), con)

    df = _sort_by_display_order_or_natural(
        df, code_col="勤務名", display_col="__display_order__"
    )
    if "__display_order__" in df.columns:
        df = df.drop(columns=["__display_order__"])

    _write_df_overwrite_sheet(
        df=df,
        out_path=out,
        sheet_name=sheet,
        template_path=template,
        start_cell="A1",
    )
    typer.echo(f"✅ 勤務種別(正社員) {len(df)}行 → {out}（{sheet}）")


@app.command("export-sheet-jobtypes-fixedterm")
def export_sheet_jobtypes_fixedterm(
    out: Path = typer.Option(Path("excel_out/班データ統合.xlsx"), "--out"),
    template: Path = typer.Option(None, "--template"),
    sheet: str = typer.Option("勤務種別(期間雇用)", "--sheet"),
):
    eng = _engine()
    JT = _resolve_jobtype_table(eng)
    jt_for_inspect = _strip_quotes(JT)

    has_disp = _has_column(eng, jt_for_inspect, "display_order")
    disp_select = (
        'display_order AS "__display_order__",'
        if has_disp
        else 'NULL AS "__display_order__",'
    )

    sql = f"""
      SELECT
             {disp_select}
             job_name   AS "勤務名",
             start_time AS "就労開始時間",
             end_time   AS "就労終了時間",
             work_hours AS "勤務時間"
        FROM {JT}
       WHERE lower(COALESCE(classification,'')) IN ('contract','part-time','pt','temp','dispatch','期間雇用社員')
    """

    with eng.connect() as con:
        df = pd.read_sql(text(sql), con)

    df = _sort_by_display_order_or_natural(
        df, code_col="勤務名", display_col="__display_order__"
    )
    if "__display_order__" in df.columns:
        df = df.drop(columns=["__display_order__"])

    _write_df_overwrite_sheet(
        df=df,
        out_path=out,
        sheet_name=sheet,
        template_path=template,
        start_cell="A1",
    )
    typer.echo(f"✅ 勤務種別(期間雇用) {len(df)}行 → {out}（{sheet}）")


@app.command("export-sheet-leavetypes")
def export_sheet_leavetypes(
    out: Path = typer.Option(Path("excel_out/班データ統合.xlsx"), "--out"),
    template: Path = typer.Option(None, "--template"),
    sheet: str = typer.Option("休暇種類", "--sheet"),
):
    eng = _engine()
    sql = """
      SELECT
        leave_name     AS "休暇名",
        leave_code     AS "休暇コード",
        leave_category AS "休暇種類"
      FROM leavetype
      ORDER BY leave_category, leave_code, leave_name
    """
    with eng.connect() as con:
        df = pd.read_sql(text(sql), con)

    _write_df_overwrite_sheet(
        df=df,
        out_path=out,
        sheet_name=sheet,
        template_path=template,
        start_cell="A1",
    )
    typer.echo(f"✅ 休暇種類 {len(df)}行 → {out}（{sheet}）")


@app.command("export-sheet-special-attendance")
def export_sheet_special_attendance(
    out: Path = typer.Option(Path("excel_out/班データ統合.xlsx"), "--out"),
    template: Path = typer.Option(None, "--template"),
    sheet: str = typer.Option("特殊区分", "--sheet"),
):
    eng = _engine()
    sql = """
      SELECT
        attendance_name   AS "区分名",
        attendance_code   AS "区分コード",
        holiday_work_flag AS "休日勤務",
        is_active         AS "有効"
      FROM special_attendance_type
      WHERE COALESCE(is_active, 1) <> 0
      ORDER BY attendance_code, attendance_name
    """
    with eng.connect() as con:
        df = pd.read_sql(text(sql), con)

    _write_df_overwrite_sheet(
        df=df,
        out_path=out,
        sheet_name=sheet,
        template_path=template,
        start_cell="A1",
    )
    typer.echo(f"✅ 特殊区分 {len(df)}行 → {out}（{sheet}）")


@app.command("export-team-workbook")
def export_team_workbook(
    department_code: str = typer.Option(
        ..., "--department-code", "-dc", help="例: DPT-A"
    ),
    team: str = typer.Option(..., "--team", "-t", help="班名（例: 1班）"),
    out: Path = typer.Option(
        Path("excel_templates/班統合データ.xlsm"), "--out", help="出力先 .xlsm"
    ),
    template: Path = typer.Option(
        Path("excel_templates/分担予定表(案).xlsm"),
        "--template",
        help="テンプレート（xlsmを指定すればマクロ保持。分担予定表(案)が入っていること）",
    ),
    # ▼ 後方互換用
    db_url: Annotated[str | None, typer.Option("--db-url")] = None,
    sqlite: Annotated[
        Path | None, typer.Option("--sqlite", help="sqlite DB ファイルパス")
    ] = None,
    plan_sheet_name: Annotated[
        str, typer.Option("--plan-sheet-name", help="テンプレ内のシート名確認用")
    ] = "分担予定表(案)",
):
    """
    部署・班ごとの Excel ブックをテンプレ込みで統合出力。
    シート構成（テンプレ保持）：
      - 分担予定表(案)
      - 社員 / 区情報 / 社員別需要 / 正社員服務表 / 期間雇用社員服務表 / 休暇種類 / 特殊区分
    """
    if sqlite is not None:
        os.environ["DATABASE_URL"] = f"sqlite:///{Path(sqlite).resolve()}"
    elif db_url:
        os.environ["DATABASE_URL"] = db_url

    typer.echo(f"▶ 班「{team}」の統合Excelを作成中...")

    _prepare_out_from_template(out, template)
    typer.echo(f"  - テンプレ基盤: {template} → {out}")

    # （任意）テンプレ検証
    try:
        import openpyxl

        wb = openpyxl.load_workbook(out, keep_vba=True, read_only=True)
        if plan_sheet_name not in wb.sheetnames:
            typer.secho(
                f"WARNING: 出力ブックに '{plan_sheet_name}' シートが見つかりません。テンプレの確認をしてください。",
                err=True,
            )
        wb.close()
    except Exception:
        pass

    # ★重要：二重出力防止のため、各シートは「上書き」で出す
    export_sheet_employees(
        department_code=department_code, team=team, out=out, sheet="社員", template=out
    )
    export_sheet_zones(
        department_code=department_code,
        team=team,
        out=out,
        sheet="区情報",
        template=out,
    )
    export_sheet_employee_demand(team=team, out=out, template=out, sheet="社員別需要")
    export_sheet_jobtypes_regular(out=out, template=out, sheet="正社員服務表")
    export_sheet_jobtypes_fixedterm(out=out, template=out, sheet="期間雇用社員服務表")
    export_sheet_leavetypes(out=out, template=out, sheet="休暇種類")
    export_sheet_special_attendance(out=out, template=out, sheet="特殊区分")

    # 仕上げ：休暇種類・特殊区分シートを VeryHidden / シート順を整える
    try:
        import openpyxl

        wb = openpyxl.load_workbook(out, keep_vba=True)

        # sheet order（見たい順）
        desired = [
            "分担予定表(案)",
            "社員",
            "区情報",
            "社員別需要",
            "正社員服務表",
            "期間雇用社員服務表",
            "休暇種類",
            "特殊区分",
        ]
        ordered_sheets = [wb[n] for n in desired if n in wb.sheetnames]
        rest = [ws for ws in wb.worksheets if ws.title not in desired]
        # openpyxl の実用的手段（private属性アクセス）
        wb._sheets = ordered_sheets + rest  # noqa: SLF001

        for s in ("休暇種類", "特殊区分"):
            if s in wb.sheetnames:
                wb[s].sheet_state = "veryHidden"

        wb.save(out)
        wb.close()
    except Exception as e:
        typer.secho(f"NOTE: 後処理に失敗: {e}", err=True)

    typer.echo(f"✅ 統合完了: {out.resolve()}")


@app.command("import-excel")
def import_excel(
    file: Path = typer.Option(
        ..., "--file", exists=True, help="編集済みの班ファイル .xlsx"
    ),
):
    """
    Excel から DB へ取り込み（UPSERT）
    - C) 社員: employee（部/班マスタも補完）
    - B) 区情報: zone → demandprofile
    - A) 社員別需要: employee_availabilities/ employeezoneproficiency
    - D) 休暇種類: leavetype（シート名: 休暇種類 / 列: 休暇コード, 休暇名, 休暇種類）※シートが無ければスキップ
    - E) 特殊区分: special_attendance_type（シート名: 特殊区分 / 列: 区分コード, 区分名, 休日勤務, 有効）※シートが無ければスキップ
    """
    eng = _engine()
    xls = pd.ExcelFile(file)

    m = re.search(r"(\d+)班", file.name)
    team_name = f"{m.group(1)}班" if m else None

    with eng.begin() as con:
        if eng.url.get_backend_name() == "sqlite":
            con.exec_driver_sql("PRAGMA journal_mode=WAL;")
            con.exec_driver_sql("PRAGMA busy_timeout=5000;")

        # ---------- C) 社員 ----------
        if "社員" in xls.sheet_names:
            df_emp = pd.read_excel(xls, sheet_name="社員").rename(
                columns={
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
                }
            )

            for c in ["department_name", "team_name"]:
                if c in df_emp.columns:
                    df_emp[c] = df_emp[c].fillna("").astype(str).str.strip()

            dep_names = sorted(
                {n for n in df_emp.get("department_name", []).tolist() if n}
            )
            team_pairs = sorted(
                {
                    (r["department_name"], r["team_name"])
                    for _, r in df_emp[["department_name", "team_name"]].iterrows()
                    if str(r.get("team_name", "")).strip()
                }
            )

            for dn in dep_names:
                dep_id = con.execute(
                    text(
                        "SELECT department_id FROM department WHERE department_name=:dn"
                    ),
                    {"dn": dn},
                ).scalar_one_or_none()
                if dep_id is None:
                    con.execute(
                        text("INSERT INTO department (department_name) VALUES (:dn)"),
                        {"dn": dn},
                    )

            for dn, tn in team_pairs:
                dep_id = None
                if dn:
                    dep_id = con.execute(
                        text(
                            "SELECT department_id FROM department WHERE department_name=:dn"
                        ),
                        {"dn": dn},
                    ).scalar_one_or_none()
                    if dep_id is None:
                        con.execute(
                            text(
                                "INSERT INTO department (department_name) VALUES (:dn)"
                            ),
                            {"dn": dn},
                        )
                        dep_id = con.execute(
                            text(
                                "SELECT department_id FROM department WHERE department_name=:dn"
                            ),
                            {"dn": dn},
                        ).scalar_one()
                team_id = con.execute(
                    text("SELECT team_id FROM team WHERE team_name=:tn"), {"tn": tn}
                ).scalar_one_or_none()
                if team_id is None:
                    con.execute(
                        text(
                            "INSERT INTO team (team_name, department_id) VALUES (:tn, :did)"
                        ),
                        {"tn": tn, "did": dep_id},
                    )

            teams = pd.read_sql(text("SELECT team_id, team_name FROM team"), con)
            tmap = teams.set_index("team_name")["team_id"].to_dict()

            def to_bool(x):
                s = str(x).strip()
                return s in (
                    "True",
                    "TRUE",
                    "true",
                    "1",
                    "○",
                    "◯",
                    "Yes",
                    "YES",
                    "はい",
                )

            for c in ["is_leader", "is_vice_leader", "is_certifier"]:
                if c in df_emp.columns:
                    df_emp[c] = df_emp[c].apply(to_bool)

            for c in ["default_work_hours", "monthly_work_hours"]:
                if c in df_emp.columns:
                    df_emp[c] = pd.to_numeric(df_emp[c], errors="coerce")

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
                    "dwh": None
                    if pd.isna(r.get("default_work_hours"))
                    else float(r.get("default_work_hours")),
                    "mwh": None
                    if pd.isna(r.get("monthly_work_hours"))
                    else float(r.get("monthly_work_hours")),
                    "team_id": team_id,
                }

                con.execute(
                    text(
                        """
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
                """
                    ),
                    payload,
                )

        # ---------- B) 区情報 ----------
        if "区情報" in xls.sheet_names:
            dfz = pd.read_excel(xls, sheet_name="区情報").rename(
                columns={
                    "区コード": "zone_code",
                    "区名": "zone_name",
                    "班": "team_name",
                    "稼働": "operational_status",
                    "月": "mon",
                    "火": "tue",
                    "水": "wed",
                    "木": "thu",
                    "金": "fri",
                    "土": "sat",
                    "日": "sun",
                    "祝": "holiday",
                    "シフトタイプ": "shift_type",
                }
            )
            for c in [
                "team_name",
                "zone_code",
                "zone_name",
                "operational_status",
                "shift_type",
            ]:
                if c in dfz.columns:
                    dfz[c] = dfz[c].fillna("").astype(str).str.strip()

            for tn in sorted({t for t in dfz.get("team_name", []).tolist() if t}):
                tid = con.execute(
                    text("SELECT team_id FROM team WHERE team_name=:tn"), {"tn": tn}
                ).scalar_one_or_none()
                if tid is None:
                    con.execute(
                        text("INSERT INTO team (team_name) VALUES (:tn)"), {"tn": tn}
                    )

            for _, r in dfz.iterrows():
                tn = r.get("team_name")
                zc = r.get("zone_code")
                zn = r.get("zone_name")
                op = r.get("operational_status")
                st = r.get("shift_type")
                if not tn:
                    continue
                tid = con.execute(
                    text("SELECT team_id FROM team WHERE team_name=:tn"), {"tn": tn}
                ).scalar_one_or_none()
                if not tid:
                    continue
                if zc:
                    zid = con.execute(
                        text("SELECT zone_id FROM zone WHERE zone_code=:zc"), {"zc": zc}
                    ).scalar_one_or_none()
                    if zid is None:
                        con.execute(
                            text(
                                """
                            INSERT INTO zone (team_id, zone_code, zone_name, operational_status, shift_type)
                            VALUES (:tid,:zc,:zn,:op,:st)
                        """
                            ),
                            {"tid": tid, "zc": zc, "zn": zn, "op": op, "st": st},
                        )
                else:
                    zid = con.execute(
                        text(
                            """
                        SELECT z.zone_id FROM zone z
                         JOIN team t ON t.team_id = z.team_id
                        WHERE t.team_name = :tn AND z.zone_name = :zn
                    """
                        ),
                        {"tn": tn, "zn": zn},
                    ).scalar_one_or_none()
                    if zid is None:
                        con.execute(
                            text(
                                """
                            INSERT INTO zone (team_id, zone_name, operational_status, shift_type)
                        VALUES (:tid,:zn,:op,:st)
                        """
                            ),
                            {"tid": tid, "zn": zn, "op": op, "st": st},
                        )

            zones2 = pd.read_sql(
                text("SELECT zone_id, zone_code, zone_name, team_id FROM zone"), con
            )
            zmap_code = zones2.set_index("zone_code")["zone_id"].dropna().to_dict()
            zones2["tn"] = (
                zones2["team_id"].astype(str) + "||" + zones2["zone_name"].fillna("")
            )
            zmap_name = zones2.set_index("tn")["zone_id"].to_dict()

            for _, r in dfz.iterrows():
                tn = str(r.get("team_name") or "").strip()
                zc = str(r.get("zone_code") or "").strip()
                zn = str(r.get("zone_name") or "").strip()

                zid = None
                if zc:
                    zid = zmap_code.get(zc)
                if zid is None and tn and zn:
                    tid = con.execute(
                        text("SELECT team_id FROM team WHERE team_name=:tn"), {"tn": tn}
                    ).scalar_one_or_none()
                    if tid:
                        key = f"{tid}||{zn}"
                        zid = zmap_name.get(key)
                if not zid:
                    continue

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
                con.execute(
                    text(
                        """
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
                """
                    ),
                    payload,
                )

        # ---------- A) 社員別需要 ----------
        if "社員別需要" in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name="社員別需要").rename(
                columns={
                    "社員番号": "employee_code",
                    "氏名": "name",
                    "月": "mon",
                    "火": "tue",
                    "水": "wed",
                    "木": "thu",
                    "金": "fri",
                    "土": "sat",
                    "日": "sun",
                    "祝": "hol",
                }
            )
            fixed = {
                "employee_code",
                "name",
                "mon",
                "tue",
                "wed",
                "thu",
                "fri",
                "sat",
                "sun",
                "hol",
            }
            zone_name_cols = [c for c in df.columns if c not in fixed]

            emp = pd.read_sql(
                text("SELECT employee_id, employee_code FROM employee"), con
            )
            emap = emp.set_index("employee_code")["employee_id"].astype(int).to_dict()

            zones = pd.read_sql(
                text(
                    """
                SELECT z.zone_id, z.zone_name, t.team_name
                  FROM zone z JOIN team t ON z.team_id=t.team_id
            """
                ),
                con,
            )
            zmap = (
                zones.set_index(["team_name", "zone_name"])["zone_id"]
                .astype(int)
                .to_dict()
            )

            if all(
                c in df.columns
                for c in ["mon", "tue", "wed", "thu", "fri", "sat", "sun", "hol"]
            ):
                for _, r in df.iterrows():
                    eid = emap.get(str(r.get("employee_code")))
                    if eid is None:
                        continue
                    vals = {
                        c: bool(r.get(c))
                        for c in [
                            "mon",
                            "tue",
                            "wed",
                            "thu",
                            "fri",
                            "sat",
                            "sun",
                            "hol",
                        ]
                    }
                    con.execute(
                        text(
                            """
                        INSERT INTO employee_availabilities
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
                    """
                        ),
                        {"eid": int(eid), **vals},
                    )

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
                    con.execute(
                        text(
                            """
                        INSERT INTO employeezoneproficiency (employee_id, zone_id, proficiency)
                        VALUES (:eid,:zid,:p)
                        ON CONFLICT (employee_id, zone_id) DO UPDATE SET proficiency=EXCLUDED.proficiency
                    """
                        ),
                        {"eid": int(eid), "zid": int(zid), "p": int(v)},
                    )

        # ---------- D) 休暇種類 ----------
        if _has_table(eng, "leavetype") and "休暇種類" in xls.sheet_names:
            df_raw = pd.read_excel(xls, sheet_name="休暇種類")
            expected = ["休暇コード", "休暇名", "休暇種類"]
            missing = [c for c in expected if c not in df_raw.columns]
            if missing:
                raise ValueError(
                    f"休暇種類シートに必須列がありません: {missing}（想定: {expected}）"
                )

            df_lt = df_raw.rename(
                columns={
                    "休暇コード": "leave_code",
                    "休暇名": "leave_name",
                    "休暇種類": "leave_category",
                }
            )[["leave_code", "leave_name", "leave_category"]].copy()

            for c in ["leave_code", "leave_name", "leave_category"]:
                df_lt[c] = df_lt[c].astype(str).str.strip()

            df_lt = df_lt[df_lt["leave_code"] != ""]

            for _, r in df_lt.iterrows():
                con.execute(
                    text(
                        """
                    INSERT INTO leavetype (leave_code, leave_name, leave_category, updated_at)
                    VALUES (:code, :name, :cat, CURRENT_TIMESTAMP)
                    ON CONFLICT (leave_code) DO UPDATE SET
                      leave_name     = EXCLUDED.leave_name,
                      leave_category = EXCLUDED.leave_category,
                      updated_at     = CURRENT_TIMESTAMP
                """
                    ),
                    {
                        "code": r.leave_code,
                        "name": r.leave_name,
                        "cat": r.leave_category,
                    },
                )

        # ---------- E) 特殊区分 ----------
        if _has_table(eng, "special_attendance_type") and "特殊区分" in xls.sheet_names:
            df_raw = pd.read_excel(xls, sheet_name="特殊区分")
            expected = ["区分コード", "区分名", "休日勤務", "有効"]
            missing = [c for c in expected if c not in df_raw.columns]
            if missing:
                raise ValueError(
                    f"特殊区分シートに必須列がありません: {missing}（想定: {expected}）"
                )

            df_sat = df_raw.rename(
                columns={
                    "区分コード": "attendance_code",
                    "区分名": "attendance_name",
                    "休日勤務": "holiday_work_flag",
                    "有効": "is_active",
                }
            )[
                ["attendance_code", "attendance_name", "holiday_work_flag", "is_active"]
            ].copy()

            for c in ["attendance_code", "attendance_name"]:
                df_sat[c] = df_sat[c].astype(str).str.strip()

            df_sat["holiday_work_flag"] = (
                df_sat["holiday_work_flag"].map(_truthy_int).fillna(0).astype(int)
            )
            df_sat["is_active"] = (
                df_sat["is_active"].map(_truthy_int).fillna(1).astype(int)
            )

            df_sat = df_sat[df_sat["attendance_code"] != ""]

            for _, r in df_sat.iterrows():
                con.execute(
                    text(
                        """
                    INSERT INTO special_attendance_type
                      (attendance_code, attendance_name, holiday_work_flag, is_active, updated_at)
                    VALUES (:code, :name, :hol, :act, CURRENT_TIMESTAMP)
                    ON CONFLICT (attendance_code) DO UPDATE SET
                      attendance_name   = EXCLUDED.attendance_name,
                      holiday_work_flag = EXCLUDED.holiday_work_flag,
                      is_active         = EXCLUDED.is_active,
                      updated_at        = CURRENT_TIMESTAMP
                """
                    ),
                    {
                        "code": str(r.attendance_code),
                        "name": str(r.attendance_name),
                        "hol": int(r.holiday_work_flag),
                        "act": int(r.is_active),
                    },
                )

    typer.echo(f"✅ Imported to DB from: {file}")


if __name__ == "__main__":
    app()
