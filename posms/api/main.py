# posms/api/main.py
from __future__ import annotations

import os
import io
import csv
import datetime as dt

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse, Response
from starlette.middleware.gzip import GZipMiddleware
from sqlalchemy import create_engine, text
from fastapi import Query

import jpholiday  # 祝日判定（ローカル）
import tempfile, subprocess, sys, traceback

# 1) app を先に作る
app = FastAPI(title="posms-api", docs_url="/docs", redoc_url=None)

# 2) ミドルウェアを app 生成「後」に追加
app.add_middleware(GZipMiddleware, minimum_size=1024)

# 3) DB は“必要になったら作る”遅延生成に変更（オフライン時も /calendar は動くように）
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://posms@db:5432/posms",
)
_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return _engine


# ---- 祝日カレンダー（28日）: Excel から直接使える CSV ----
WEEKDAY_JP = ["月", "火", "水", "木", "金", "土", "日"]


@app.get("/calendar.csv", response_class=PlainTextResponse)
def calendar_csv(start: str):
    """開始日から 28 日分のカレンダー（週末/祝日フラグ入り）を CSV で返す。"""
    try:
        s = dt.date.fromisoformat(start)
    except Exception:
        raise HTTPException(400, "start must be YYYY-MM-DD")
    rows = []
    for i in range(28):
        d = s + dt.timedelta(days=i)
        dow = d.weekday()  # 0=Mon..6=Sun
        is_weekend = dow >= 5
        hol_name = jpholiday.is_holiday_name(d) or ""
        is_holiday = bool(hol_name)
        rows.append(
            [
                d.isoformat(),
                WEEKDAY_JP[dow],
                d.day,
                int(is_weekend),
                int(is_holiday),
                hol_name,
                int(is_weekend or is_holiday),
            ]
        )
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        ["date", "weekday", "day", "is_weekend", "is_holiday", "holiday_name", "is_off"]
    )
    w.writerows(rows)
    # 末尾の return を UTF-8 に変更（UTF-16LE → UTF-8 with BOM）
    csv_text = buf.getvalue()
    return Response(
        content=("﻿" + csv_text).encode("utf-8"),  # ← 先頭の"﻿"は UTF-8 BOM
        media_type="text/csv; charset=utf-8",
    )

# ---- ヘルスチェック：DB が無くても 200 を返し、状態をフィールドで伝える ----
@app.get("/health")
def health():
    db_ok = False
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
            db_ok = True
    except Exception:
        db_ok = False
    return {"ok": True, "db": db_ok}


# --- DB → CSV（祝日） ---
@app.get("/export/holidays.csv", response_class=PlainTextResponse)
def export_holidays_csv():
    sql = """
      SELECT holiday_date::date
      FROM holidays
      WHERE is_holiday = TRUE
      ORDER BY holiday_date
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["holiday_date"])
    for (d,) in rows:
        w.writerow([d.strftime("%Y-%m-%d")])
    csv_bytes = ("\ufeff" + buf.getvalue()).encode("utf-16le")
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-16")


# --- CSV → DB（祝日 UPSERT） ---
@app.post("/import/holidays.csv")
async def import_holidays_csv(req: Request):
    content = (await req.body()).decode("utf-8-sig", "ignore")
    rdr = csv.DictReader(io.StringIO(content))
    upserted = 0
    with get_engine().begin() as conn:
        for row in rdr:
            if not row.get("holiday_date"):
                continue
            d = dt.date.fromisoformat(row["holiday_date"].strip())
            conn.execute(
                text(
                    """
                INSERT INTO holidays (holiday_date, is_holiday)
                VALUES (:d, TRUE)
                ON CONFLICT (holiday_date) DO UPDATE
                SET is_holiday = EXCLUDED.is_holiday
            """
                ),
                {"d": d},
            )
            upserted += 1
    return JSONResponse({"status": "ok", "upserted": upserted})


# --- 班一覧（部署名付き）CSV ---
@app.get("/export/teams.csv", response_class=PlainTextResponse)
def export_teams_csv(only_active: bool = True):
    sql = """
      SELECT t.team_id, d.department_id, d.department_name, t.team_name
      FROM team t
      JOIN department d ON t.department_id = d.department_id
    """
    if only_active:
        sql += " WHERE COALESCE(d.is_active, TRUE) AND COALESCE(t.is_active, TRUE)"
    sql += " ORDER BY d.department_name, t.team_name"

    with get_engine().connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["team_id", "department_id", "department_name", "team_name"])
    w.writerows(rows)
    csv_bytes = ("\ufeff" + buf.getvalue()).encode("utf-16le")
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-16")


# --- 指定班の社員を CSV で返す（必要列だけ返せる） ---
@app.get("/export/employees.csv", response_class=PlainTextResponse)
def export_employees_csv(
    team_id: int | None = None,
    order: str = "id",
    fields: str | None = None,
):
    # 取り出し可能な列（安全なホワイトリスト）
    colmap = {
        "employee_id": "e.employee_id",
        "employee_code": "e.employee_code",
        "name": "e.name",
        "employment_type": "e.employment_type",
        "position": "e.position",
        "default_work_hours": "e.default_work_hours",
        "monthly_work_hours": "e.monthly_work_hours",
        "is_certifier": "e.is_certifier",
        "team_id": "e.team_id",
        "team_name": "t.team_name",
        "department_name": "d.department_name",
        # zone_name を返すなら、実際のスキーマに合わせて JOIN を追加する
        # "zone_name": "z.zone_name",
    }

    # 返す列の決定
    if fields:
        req = [s.strip() for s in fields.split(",") if s.strip()]
        sel_cols = [c for c in req if c in colmap]
        if not sel_cols:
            sel_cols = [
                "employee_id",
                "employee_code",
                "name",
                "position",
                "team_id",
                "team_name",
                "department_name",
            ]
    else:
        sel_cols = [
            "employee_id",
            "employee_code",
            "name",
            "position",
            "team_id",
            "team_name",
            "department_name",
        ]

    base = """
      SELECT {select_list}
      FROM employee e
      JOIN team t        ON e.team_id = t.team_id
      JOIN department d  ON t.department_id = d.department_id
    """

    params = {}
    if team_id is not None:
        base += " WHERE e.team_id = :team_id"
        params["team_id"] = team_id

    # 並び順
    if order == "id":
        base += " ORDER BY e.employee_id"
    elif order == "code":
        base += " ORDER BY e.employee_code"
    elif order == "name":
        base += " ORDER BY e.name"
    elif order == "natural_code":
        base += " ORDER BY COALESCE(((regexp_match(e.employee_code, '[0-9]+$'))[1])::int, 2147483647), e.employee_code"
    elif order == "natural_name":
        base += " ORDER BY COALESCE(((regexp_match(e.name, '[0-9]+$'))[1])::int, 2147483647), e.name"
    else:
        base += " ORDER BY e.employee_id"

    select_list = ", ".join(colmap[c] + f' AS "{c}"' for c in sel_cols)
    sql = base.format(select_list=select_list)

    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(sel_cols)
    for row in rows:
        w.writerow([row[i] if row[i] is not None else "" for i in range(len(sel_cols))])

    csv_bytes = ("\ufeff" + buf.getvalue()).encode("utf-16le")
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-16")
    
# === team/employees.csv（自然キー：department_code or department_name と team_name） ===

def _has_col(conn, table: str, column: str) -> bool:
    q = text("""
        select 1
          from information_schema.columns
         where table_name = :t and column_name = :c
         limit 1
    """)
    return conn.execute(q, {"t": table, "c": column}).scalar() is not None

def _csv_utf8_bom(filename: str, headers: list[str], rows: list[list]) -> Response:
    buf = io.StringIO(newline="")
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(headers)
    w.writerows(rows)
    return Response(
        content="\ufeff" + buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@app.get("/team/employees.csv")
def team_employees_csv(
    team_name: str = Query(..., description="班の表示名（必須）"),
    department_code: str | None = Query(None, description="部署コード（どちらか片方）"),
    department_name: str | None = Query(None, description="部署名（どちらか片方）"),
):
    if not (department_code or department_name):
        raise HTTPException(400, "department_code か department_name のどちらかを指定してください")

    headers = [
        "employee_code","name","employment_type","position",
        "default_work_hours","monthly_work_hours","is_certifier","row_version"
    ]

    with get_engine().connect() as conn:
        # チーム特定：スキーマ差異に対応（department_code列が無ければ department_name を使う）
        team_id = None
        if department_code:
            if not _has_col(conn, "department", "department_code"):
                raise HTTPException(400, "department.department_code が存在しません。department_name を指定してください。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_code = :dc and t.team_name = :tn
            """), {"dc": department_code, "tn": team_name}).scalar_one_or_none()
        else:
            if not _has_col(conn, "department", "department_name"):
                raise HTTPException(400, "department.department_name 列が見つかりません。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_name = :dn and t.team_name = :tn
            """), {"dn": department_name, "tn": team_name}).scalar_one_or_none()

        if team_id is None:
            raise HTTPException(404, "team not found")

        has_row_version = _has_col(conn, "employee", "row_version")
        if has_row_version:
            sql = """
                select e.employee_code, e.name, e.employment_type, e.position,
                       e.default_work_hours, e.monthly_work_hours, e.is_certifier, e.row_version
                  from employee e
                 where e.team_id = :tid
                 order by e.employee_code
            """
            rows = conn.execute(text(sql), {"tid": team_id}).all()
            data = [list(r) for r in rows]
        else:
            sql = """
                select e.employee_code, e.name, e.employment_type, e.position,
                       e.default_work_hours, e.monthly_work_hours, e.is_certifier
                  from employee e
                 where e.team_id = :tid
                 order by e.employee_code
            """
            rows = conn.execute(text(sql), {"tid": team_id}).all()
            data = [list(r) + [0] for r in rows]  # row_version が無い場合は 0 で埋める

    return _csv_utf8_bom("employees.csv", headers, data)
# ==============================================================================
# === team/availability.csv（自然キー：department_code または department_name + team_name） ===

def _has_table(conn, table: str) -> bool:
    q = text("""
        select 1 from information_schema.tables
         where table_name = :t limit 1
    """)
    return conn.execute(q, {"t": table}).scalar() is not None

# 既に _has_col / _csv_utf8_bom が無い環境向けの保険（employees.csvパッチ未適用時）
try:
    _has_col  # type: ignore
except NameError:
    def _has_col(conn, table: str, column: str) -> bool:
        q = text("""
            select 1 from information_schema.columns
             where table_name = :t and column_name = :c limit 1
        """)
        return conn.execute(q, {"t": table, "c": column}).scalar() is not None

try:
    _csv_utf8_bom  # type: ignore
except NameError:
    def _csv_utf8_bom(filename: str, headers: list[str], rows: list[list]) -> Response:
        buf = io.StringIO(newline="")
        w = csv.writer(buf, lineterminator="\r\n")
        w.writerow(headers)
        w.writerows(rows)
        return Response(
            content="\ufeff" + buf.getvalue(),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

@app.get("/team/availability.csv")
def team_availability_csv(
    team_name: str = Query(..., description="班の表示名（必須）"),
    department_code: str | None = Query(None, description="部署コード（どちらか片方）"),
    department_name: str | None = Query(None, description="部署名（どちらか片方）"),
):
    if not (department_code or department_name):
        raise HTTPException(400, "department_code か department_name のどちらかを指定してください")

    headers = [
        "employee_code",
        "available_mon","available_tue","available_wed","available_thu",
        "available_fri","available_sat","available_sun","available_hol",
        "row_version",
    ]

    with get_engine().connect() as conn:
        # --- チーム特定（department_code が無ければ department_name で特定） ---
        if department_code:
            if not _has_col(conn, "department", "department_code"):
                raise HTTPException(400, "department.department_code が存在しません。department_name を指定してください。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_code = :dc and t.team_name = :tn
            """), {"dc": department_code, "tn": team_name}).scalar_one_or_none()
        else:
            if not _has_col(conn, "department", "department_name"):
                raise HTTPException(400, "department.department_name 列が見つかりません。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_name = :dn and t.team_name = :tn
            """), {"dn": department_name, "tn": team_name}).scalar_one_or_none()

        if team_id is None:
            raise HTTPException(404, "team not found")

        # --- availability テーブル有無で分岐（無ければ全True・row_version=0で返す） ---
        if _has_table(conn, "employee_availability"):
            has_row_version = _has_col(conn, "employee_availability", "row_version")
            sql = f"""
                select e.employee_code,
                       coalesce(av.available_mon,  true) as available_mon,
                       coalesce(av.available_tue,  true) as available_tue,
                       coalesce(av.available_wed,  true) as available_wed,
                       coalesce(av.available_thu,  true) as available_thu,
                       coalesce(av.available_fri,  true) as available_fri,
                       coalesce(av.available_sat,  true) as available_sat,
                       coalesce(av.available_sun,  true) as available_sun,
                       coalesce(av.available_hol,  true) as available_hol,
                       {("coalesce(av.row_version, 0)" if has_row_version else "0")} as row_version
                  from employee e
                  left join employee_availability av on av.employee_id = e.employee_id
                 where e.team_id = :tid
                 order by e.employee_code
            """
            rows = conn.execute(text(sql), {"tid": team_id}).all()
            data = [list(r) for r in rows]
        else:
            # availability テーブルが無い場合でも、外形を固定して返す
            emp_rows = conn.execute(text("""
                select e.employee_code
                  from employee e
                 where e.team_id = :tid
                 order by e.employee_code
            """), {"tid": team_id}).all()
            data = [
                [r[0], True, True, True, True, True, True, True, True, 0]
                for r in emp_rows
            ]

    return _csv_utf8_bom("availability.csv", headers, data)
# ==============================================================================
# === team/zones_demand.csv（Zone + DemandProfile 統合）========================
@app.get("/team/zones_demand.csv")
def team_zones_demand_csv(
    team_name: str = Query(..., description="班の表示名（必須）"),
    department_code: str | None = Query(None, description="部署コード（どちらか片方）"),
    department_name: str | None = Query(None, description="部署名（どちらか片方）"),
):
    if not (department_code or department_name):
        raise HTTPException(400, "department_code か department_name のどちらかを指定してください")

    headers = [
        "zone_code","zone_name","operational_status",
        "demand_mon","demand_tue","demand_wed","demand_thu",
        "demand_fri","demand_sat","demand_sun","demand_holiday",
        "row_version",
    ]

    def _first_existing_table(conn, names: list[str]) -> str | None:
        for n in names:
            if _has_table(conn, n):
                return n
        return None

    with get_engine().connect() as conn:
        # --- team_id 特定 ---
        if department_code:
            if not _has_col(conn, "department", "department_code"):
                raise HTTPException(400, "department.department_code が存在しません。department_name を指定してください。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_code = :dc and t.team_name = :tn
            """), {"dc": department_code, "tn": team_name}).scalar_one_or_none()
        else:
            if not _has_col(conn, "department", "department_name"):
                raise HTTPException(400, "department.department_name 列が見つかりません。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_name = :dn and t.team_name = :tn
            """), {"dn": department_name, "tn": team_name}).scalar_one_or_none()

        if team_id is None:
            raise HTTPException(404, "team not found")

        # --- zone が必須 ---
        if not _has_table(conn, "zone"):
            return _csv_utf8_bom("zones_demand.csv", headers, [])

        # zone 列の表現
        zone_code_expr = "z.zone_code as zone_code" if _has_col(conn, "zone", "zone_code") else "cast(z.zone_id as text) as zone_code"
        zone_name_expr = "z.zone_name as zone_name" if _has_col(conn, "zone", "zone_name") else "'' as zone_name"
        oper_expr      = "z.operational_status as operational_status" if _has_col(conn, "zone", "operational_status") else "'active' as operational_status"

        # --- demand テーブル名の差に対応 ---
        dp_table = _first_existing_table(conn, ["demand_profile", "demandprofile", "zone_demand"])
        dp_exists = dp_table is not None

        def dp_expr(col: str, as_name: str | None = None) -> str:
            as_name = as_name or col
            # 候補: demand_mon / mon
            if dp_exists and _has_col(conn, dp_table, col):
                return f"coalesce(d.{col}, 0) as {as_name}"
            alt = col.replace("demand_", "")  # mon/tue...
            if dp_exists and _has_col(conn, dp_table, alt):
                return f"coalesce(d.{alt}, 0) as {as_name}"
            return f"0 as {as_name}"

        # holiday 列のゆれ: demand_holiday / demand_hol / holiday
        if dp_exists and _has_col(conn, dp_table, "demand_holiday"):
            hol_expr = "coalesce(d.demand_holiday, 0) as demand_holiday"
        elif dp_exists and _has_col(conn, dp_table, "demand_hol"):
            hol_expr = "coalesce(d.demand_hol, 0) as demand_holiday"
        elif dp_exists and _has_col(conn, dp_table, "holiday"):
            hol_expr = "coalesce(d.holiday, 0) as demand_holiday"
        else:
            hol_expr = "0 as demand_holiday"

        rv_expr = "coalesce(d.row_version, 0) as row_version" if (dp_exists and _has_col(conn, dp_table, "row_version")) else "0 as row_version"

        select_list = ", ".join([
            zone_code_expr,
            zone_name_expr,
            oper_expr,
            dp_expr("demand_mon"),
            dp_expr("demand_tue"),
            dp_expr("demand_wed"),
            dp_expr("demand_thu"),
            dp_expr("demand_fri"),
            dp_expr("demand_sat"),
            dp_expr("demand_sun"),
            hol_expr,
            rv_expr,
        ])

        # --- 結合は列が揃う場合のみ ---
        join_clause = ""
        if dp_exists:
            if _has_col(conn, dp_table, "zone_id") and _has_col(conn, "zone", "zone_id"):
                join_clause = f"left join {dp_table} d on d.zone_id = z.zone_id"
            elif _has_col(conn, dp_table, "zone_code") and _has_col(conn, "zone", "zone_code"):
                join_clause = f"left join {dp_table} d on d.zone_code = z.zone_code"
            else:
                join_clause = ""  # キーが無ければJOINしない（すべて0で返す）

        order_key = "z.zone_code" if _has_col(conn, "zone", "zone_code") else "z.zone_id"

        sql = f"""
            select {select_list}
              from zone z
              {join_clause}
             where z.team_id = :tid
             order by {order_key}
        """
        rows = conn.execute(text(sql), {"tid": team_id}).all()
        data = [list(r) for r in rows]

    return _csv_utf8_bom("zones_demand.csv", headers, data)
# =============================================================================
# === team/proficiency.csv（employee_code, zone_code, proficiency, row_version） ===
@app.get("/team/proficiency.csv")
def team_proficiency_csv(
    team_name: str = Query(..., description="班の表示名（必須）"),
    department_code: str | None = Query(None, description="部署コード（どちらか片方）"),
    department_name: str | None = Query(None, description="部署名（どちらか片方）"),
):
    if not (department_code or department_name):
        raise HTTPException(400, "department_code か department_name のどちらかを指定してください")

    headers = ["employee_code", "zone_code", "proficiency", "row_version"]

    # 補助: 既存の関数が無い環境への保険
    try:
        _has_table  # type: ignore
    except NameError:
        def _has_table(conn, table: str) -> bool:
            q = text("select 1 from information_schema.tables where table_name = :t limit 1")
            return conn.execute(q, {"t": table}).scalar() is not None

    try:
        _has_col  # type: ignore
    except NameError:
        def _has_col(conn, table: str, column: str) -> bool:
            q = text("""
                select 1 from information_schema.columns
                 where table_name = :t and column_name = :c limit 1
            """)
            return conn.execute(q, {"t": table, "c": column}).scalar() is not None

    try:
        _csv_utf8_bom  # type: ignore
    except NameError:
        def _csv_utf8_bom(filename: str, headers: list[str], rows: list[list]) -> Response:
            buf = io.StringIO(newline="")
            w = csv.writer(buf, lineterminator="\r\n")
            w.writerow(headers)
            w.writerows(rows)
            return Response(
                content="\ufeff" + buf.getvalue(),
                media_type="text/csv; charset=utf-8",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'}
            )

    def _first_existing_table(conn, names: list[str]) -> str | None:
        for n in names:
            if _has_table(conn, n):
                return n
        return None

    with get_engine().connect() as conn:
        # --- team_id 特定 ---
        if department_code:
            if not _has_col(conn, "department", "department_code"):
                raise HTTPException(400, "department.department_code が存在しません。department_name を指定してください。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_code = :dc and t.team_name = :tn
            """), {"dc": department_code, "tn": team_name}).scalar_one_or_none()
        else:
            if not _has_col(conn, "department", "department_name"):
                raise HTTPException(400, "department.department_name 列が見つかりません。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_name = :dn and t.team_name = :tn
            """), {"dn": department_name, "tn": team_name}).scalar_one_or_none()

        if team_id is None:
            raise HTTPException(404, "team not found")

        # --- 必須テーブル: employee / zone（どちらか無ければ空で返す） ---
        if not _has_table(conn, "employee") or not _has_table(conn, "zone"):
            return _csv_utf8_bom("proficiency.csv", headers, [])

        # --- プロフィシエンシのテーブル名ゆれに対応 ---
        prof_table = _first_existing_table(conn, [
            "employee_zone_proficiency", "employeezoneproficiency",
            "proficiency", "emp_zone_prof", "skill_matrix"
        ])
        if not prof_table:
            # テーブル未作成なら外形だけ（空）を返す
            return _csv_utf8_bom("proficiency.csv", headers, [])

        # 列ゆれ: 値, キー
        # 値: proficiency / level / skill / proficiency_level
        if   _has_col(conn, prof_table, "proficiency"):
            prof_val = "coalesce(p.proficiency, 0) as proficiency"
        elif _has_col(conn, prof_table, "level"):
            prof_val = "coalesce(p.level, 0) as proficiency"
        elif _has_col(conn, prof_table, "skill"):
            prof_val = "coalesce(p.skill, 0) as proficiency"
        elif _has_col(conn, prof_table, "proficiency_level"):
            prof_val = "coalesce(p.proficiency_level, 0) as proficiency"
        else:
            prof_val = "0 as proficiency"

        # row_version
        rv_expr = "coalesce(p.row_version, 0) as row_version" if _has_col(conn, prof_table, "row_version") else "0 as row_version"

        # employee 結合
        if   _has_col(conn, prof_table, "employee_id") and _has_col(conn, "employee", "employee_id"):
            join_emp = "p.employee_id = e.employee_id"
        elif _has_col(conn, prof_table, "employee_code") and _has_col(conn, "employee", "employee_code"):
            join_emp = "p.employee_code = e.employee_code"
        else:
            # キーが合わなければ空を返す
            return _csv_utf8_bom("proficiency.csv", headers, [])

        # zone 結合
        if   _has_col(conn, prof_table, "zone_id") and _has_col(conn, "zone", "zone_id"):
            join_zone = "p.zone_id = z.zone_id"
        elif _has_col(conn, prof_table, "zone_code") and _has_col(conn, "zone", "zone_code"):
            join_zone = "p.zone_code = z.zone_code"
        else:
            return _csv_utf8_bom("proficiency.csv", headers, [])

        # 表示列（自然キー出力）
        zone_code_expr = "z.zone_code as zone_code" if _has_col(conn, "zone", "zone_code") else "cast(z.zone_id as text) as zone_code"

        order_emp = "e.employee_code" if _has_col(conn, "employee", "employee_code") else "e.employee_id"
        order_zone = "z.zone_code" if _has_col(conn, "zone", "zone_code") else "z.zone_id"

        sql = f"""
            select e.employee_code as employee_code,
                   {zone_code_expr},
                   {prof_val},
                   {rv_expr}
              from {prof_table} p
              join employee e on {join_emp}
              join zone z      on {join_zone}
             where e.team_id = :tid and z.team_id = :tid
             order by {order_emp}, {order_zone}
        """
        rows = conn.execute(text(sql), {"tid": team_id}).all()
        data = [list(r) for r in rows]

    return _csv_utf8_bom("proficiency.csv", headers, data)
# ==============================================================================
# === team/employees_jp.csv（日本語ヘッダ：社員コード ほか） ====================
@app.get("/team/employees_jp.csv")
def team_employees_jp_csv(
    team_name: str = Query(..., description="班の表示名（必須）"),
    department_code: str | None = Query(None, description="部署コード（どちらか片方）"),
    department_name: str | None = Query(None, description="部署名（どちらか片方）"),
):
    if not (department_code or department_name):
        raise HTTPException(400, "department_code か department_name のどちらかを指定してください")

    HEADERS_JP = ["社員番号","氏名","社員タイプ","役職","勤務時間(日)","勤務時間(月)","認証司","row_version"]

    with get_engine().connect() as conn:
        # 班IDの特定（department_code優先、無ければdepartment_name）
        if department_code:
            if not _has_col(conn, "department", "department_code"):
                raise HTTPException(400, "department.department_code が存在しません。department_name を指定してください。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_code = :dc and t.team_name = :tn
            """), {"dc": department_code, "tn": team_name}).scalar_one_or_none()
        else:
            if not _has_col(conn, "department", "department_name"):
                raise HTTPException(400, "department.department_name 列が見つかりません。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_name = :dn and t.team_name = :tn
            """), {"dn": department_name, "tn": team_name}).scalar_one_or_none()

        if team_id is None:
            raise HTTPException(404, "team not found")

        # row_version の有無で分岐（無ければ 0 埋め）
        has_row_version = _has_col(conn, "employee", "row_version")
        if has_row_version:
            rows = conn.execute(text("""
                select e.employee_code, e.name, e.employment_type, e.position,
                       e.default_work_hours, e.monthly_work_hours, e.is_certifier, e.row_version
                  from employee e
                 where e.team_id = :tid
                 order by e.employee_code
            """), {"tid": team_id}).all()
            data = [list(r) for r in rows]
        else:
            rows = conn.execute(text("""
                select e.employee_code, e.name, e.employment_type, e.position,
                       e.default_work_hours, e.monthly_work_hours, e.is_certifier
                  from employee e
                 where e.team_id = :tid
                 order by e.employee_code
            """), {"tid": team_id}).all()
            data = [list(r) + [0] for r in rows]

    return _csv_utf8_bom("employees_jp.csv", HEADERS_JP, data)
# ==============================================================================
# === team/availability_jp.csv（日本語ヘッダ版） ================================
@app.get("/team/availability_jp.csv")
def team_availability_jp_csv(
    team_name: str = Query(..., description="班の表示名（必須）"),
    department_code: str | None = Query(None, description="部署コード（どちらか片方）"),
    department_name: str | None = Query(None, description="部署名（どちらか片方）"),
):
    if not (department_code or department_name):
        raise HTTPException(400, "department_code か department_name のどちらかを指定してください")

    HEADERS_JP = ["社員番号","月","火","水","木","金","土","日","祝","row_version"]

    with get_engine().connect() as conn:
        # --- チーム特定 ---
        if department_code:
            if not _has_col(conn, "department", "department_code"):
                raise HTTPException(400, "department.department_code が存在しません。department_name を指定してください。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_code = :dc and t.team_name = :tn
            """), {"dc": department_code, "tn": team_name}).scalar_one_or_none()
        else:
            if not _has_col(conn, "department", "department_name"):
                raise HTTPException(400, "department.department_name 列が見つかりません。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_name = :dn and t.team_name = :tn
            """), {"dn": department_name, "tn": team_name}).scalar_one_or_none()

        if team_id is None:
            raise HTTPException(404, "team not found")

        # --- availability テーブル名のゆれに対応（employee_availability / employeeavailability） ---
        def _first_existing_table(names: list[str]) -> str | None:
            for n in names:
                if _has_table(conn, n):
                    return n
            return None

        av_table = _first_existing_table(["employee_availability", "employeeavailability"])

        if av_table:
            # JOINキー自動判別（employee_id 優先、なければ employee_code）
            if _has_col(conn, av_table, "employee_id") and _has_col(conn, "employee", "employee_id"):
                join_key = "av.employee_id = e.employee_id"
            elif _has_col(conn, av_table, "employee_code") and _has_col(conn, "employee", "employee_code"):
                join_key = "av.employee_code = e.employee_code"
            else:
                join_key = None
        else:
            join_key = None

        if av_table and join_key:
            has_row_version = _has_col(conn, av_table, "row_version")
            # 既定: 平日=TRUE／土日祝=FALSE
            sql = f"""
                select e.employee_code,
                       coalesce(av.available_mon,  true)  as available_mon,
                       coalesce(av.available_tue,  true)  as available_tue,
                       coalesce(av.available_wed,  true)  as available_wed,
                       coalesce(av.available_thu,  true)  as available_thu,
                       coalesce(av.available_fri,  true)  as available_fri,
                       coalesce(av.available_sat,  false) as available_sat,
                       coalesce(av.available_sun,  false) as available_sun,
                       coalesce(av.available_hol,  false) as available_hol,
                       {("coalesce(av.row_version, 0)" if has_row_version else "0")} as row_version
                  from employee e
                  left join {av_table} av on {join_key}
                 where e.team_id = :tid
                 order by e.employee_code
            """
            rows = conn.execute(text(sql), {"tid": team_id}).all()
            data = [list(r) for r in rows]
        else:
            # テーブル無し／キー不一致 → 外形固定（平日TRUE／土日祝FALSE, row_version=0）
            emp_rows = conn.execute(text("""
                select e.employee_code
                  from employee e
                 where e.team_id = :tid
                 order by e.employee_code
            """), {"tid": team_id}).all()
            data = [[r[0], True, True, True, True, True, False, False, False, 0] for r in emp_rows]

    return _csv_utf8_bom("availability_jp.csv", HEADERS_JP, data)
# ==============================================================================
# === team/zones_demand_jp.csv（日本語ヘッダ：区, 業務状態, 月..祝, row_version） ===
@app.get("/team/zones_demand_jp.csv")
def team_zones_demand_jp_csv(
    team_name: str = Query(..., description="班の表示名（必須）"),
    department_code: str | None = Query(None, description="部署コード（どちらか片方）"),
    department_name: str | None = Query(None, description="部署名（どちらか片方）"),
):
    if not (department_code or department_name):
        raise HTTPException(400, "department_code か department_name のどちらかを指定してください")

    HEADERS_JP = ["区","業務状態","月","火","水","木","金","土","日","祝","row_version"]

    with get_engine().connect() as conn:
        # --- team_id 特定 ---
        if department_code:
            if not _has_col(conn, "department", "department_code"):
                raise HTTPException(400, "department.department_code が存在しません。department_name を指定してください。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_code = :dc and t.team_name = :tn
            """), {"dc": department_code, "tn": team_name}).scalar_one_or_none()
        else:
            if not _has_col(conn, "department", "department_name"):
                raise HTTPException(400, "department.department_name 列が見つかりません。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_name = :dn and t.team_name = :tn
            """), {"dn": department_name, "tn": team_name}).scalar_one_or_none()

        if team_id is None:
            raise HTTPException(404, "team not found")

        # --- zone 必須 ---
        if not _has_table(conn, "zone"):
            return _csv_utf8_bom("zones_demand_jp.csv", HEADERS_JP, [])

        # util: 先に存在するテーブル名を返す
        def _first_existing_table(names: list[str]) -> str | None:
            for n in names:
                if _has_table(conn, n):
                    return n
            return None

        # zone名（区）を優先、無ければコード→id文字列
        if _has_col(conn, "zone", "zone_name"):
            zone_jp = "z.zone_name as 区"
            order_key = "z.zone_name"
        elif _has_col(conn, "zone", "zone_code"):
            zone_jp = "z.zone_code as 区"
            order_key = "z.zone_code"
        else:
            zone_jp = "cast(z.zone_id as text) as 区"
            order_key = "z.zone_id"

        # 業務状態（無ければ空文字）
        oper_jp = "z.operational_status as 業務状態" if _has_col(conn, "zone", "operational_status") else "'' as 業務状態"

        # demand テーブル（demand_profile / demandprofile / zone_demand）
        dp_table = _first_existing_table(["demand_profile", "demandprofile", "zone_demand"])
        dp_exists = dp_table is not None

        def dp_expr(col: str, alias: str) -> str:
            # demand_mon or mon など列ゆれ対応
            if dp_exists and _has_col(conn, dp_table, col):
                return f"coalesce(d.{col}, 0) as {alias}"
            alt = col.replace("demand_", "")
            if dp_exists and _has_col(conn, dp_table, alt):
                return f"coalesce(d.{alt}, 0) as {alias}"
            return f"0 as {alias}"

        # 祝日列のゆれ
        if dp_exists and _has_col(conn, dp_table, "demand_holiday"):
            hol_expr = "coalesce(d.demand_holiday, 0) as 祝"
        elif dp_exists and _has_col(conn, dp_table, "demand_hol"):
            hol_expr = "coalesce(d.demand_hol, 0) as 祝"
        elif dp_exists and _has_col(conn, dp_table, "holiday"):
            hol_expr = "coalesce(d.holiday, 0) as 祝"
        else:
            hol_expr = "0 as 祝"

        rv_expr = "coalesce(d.row_version, 0) as row_version" if (dp_exists and _has_col(conn, dp_table, "row_version")) else "0 as row_version"

        select_list = ", ".join([
            zone_jp,
            oper_jp,
            dp_expr("demand_mon", "月"),
            dp_expr("demand_tue", "火"),
            dp_expr("demand_wed", "水"),
            dp_expr("demand_thu", "木"),
            dp_expr("demand_fri", "金"),
            dp_expr("demand_sat", "土"),
            dp_expr("demand_sun", "日"),
            hol_expr,
            rv_expr,
        ])

        # 結合は列が揃う場合のみ
        join_clause = ""
        if dp_exists:
            if _has_col(conn, dp_table, "zone_id") and _has_col(conn, "zone", "zone_id"):
                join_clause = f"left join {dp_table} d on d.zone_id = z.zone_id"
            elif _has_col(conn, dp_table, "zone_code") and _has_col(conn, "zone", "zone_code"):
                join_clause = f"left join {dp_table} d on d.zone_code = z.zone_code"

        sql = f"""
            select {select_list}
              from zone z
              {join_clause}
             where z.team_id = :tid
             order by {order_key}
        """
        rows = conn.execute(text(sql), {"tid": team_id}).all()
        data = [list(r) for r in rows]

    return _csv_utf8_bom("zones_demand_jp.csv", HEADERS_JP, data)
# ==============================================================================
# 置き換え版: /team/proficiency_jp.csv（社員番号, 区, 優先順位, row_version）
@app.get("/team/proficiency_jp.csv")
def team_proficiency_jp_csv(
    team_name: str = Query(..., description="班の表示名（必須）"),
    department_code: str | None = Query(None, description="部署コード（どちらか片方）"),
    department_name: str | None = Query(None, description="部署名（どちらか片方）"),
):
    if not (department_code or department_name):
        raise HTTPException(400, "department_code か department_name のどちらかを指定してください")

    HEADERS_JP = ["社員番号", "区", "優先順位", "row_version"]

    with get_engine().connect() as conn:
        # --- team_id 特定 ---
        if department_code:
            if not _has_col(conn, "department", "department_code"):
                raise HTTPException(400, "department.department_code が存在しません。department_name を指定してください。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_code = :dc and t.team_name = :tn
            """), {"dc": department_code, "tn": team_name}).scalar_one_or_none()
        else:
            if not _has_col(conn, "department", "department_name"):
                raise HTTPException(400, "department.department_name 列が見つかりません。")
            team_id = conn.execute(text("""
                select t.team_id
                  from team t
                  join department d on d.department_id = t.department_id
                 where d.department_name = :dn and t.team_name = :tn
            """), {"dn": department_name, "tn": team_name}).scalar_one_or_none()

        if team_id is None:
            raise HTTPException(404, "team not found")

        # --- 必須: employee / zone が無ければ空で返す ---
        if not _has_table(conn, "employee") or not _has_table(conn, "zone"):
            return _csv_utf8_bom("proficiency_jp.csv", HEADERS_JP, [])

        # 先に存在するテーブル名を選ぶ（従来の「習熟度」テーブル流用を想定）
        def _first_existing_table(names: list[str]) -> str | None:
            for n in names:
                if _has_table(conn, n):
                    return n
            return None

        prof_table = _first_existing_table([
            "employee_zone_proficiency", "employeezoneproficiency",
            "proficiency", "emp_zone_prof", "skill_matrix",
            "employee_zone_priority", "employeezonepriority", "preference", "zone_preference"
        ])
        if not prof_table:
            return _csv_utf8_bom("proficiency_jp.csv", HEADERS_JP, [])

        # 値カラムのゆれを優先順位で吸収（なければ proficiency → 最後は 0）
        def priority_expr() -> str:
            candidates = [
                "priority", "priority_rank", "priority_order",
                "rank", "rank_no",
                "preference", "preference_rank", "preference_order", "pref_order",
            ]
            for c in candidates:
                if _has_col(conn, prof_table, c):
                    return f"coalesce(p.{c}, 0) as 優先順位"
            # フォールバック：proficiency を優先順位として扱う（無ければ 0）
            if _has_col(conn, prof_table, "proficiency"):
                return "coalesce(p.proficiency, 0) as 優先順位"
            return "0 as 優先順位"

        rv_expr = "coalesce(p.row_version, 0) as row_version" if _has_col(conn, prof_table, "row_version") else "0 as row_version"

        # JOINキー（employee / zone）
        if   _has_col(conn, prof_table, "employee_id") and _has_col(conn, "employee", "employee_id"):
            join_emp = "p.employee_id = e.employee_id"
        elif _has_col(conn, prof_table, "employee_code") and _has_col(conn, "employee", "employee_code"):
            join_emp = "p.employee_code = e.employee_code"
        else:
            return _csv_utf8_bom("proficiency_jp.csv", HEADERS_JP, [])

        if   _has_col(conn, prof_table, "zone_id") and _has_col(conn, "zone", "zone_id"):
            join_zone = "p.zone_id = z.zone_id"
        elif _has_col(conn, prof_table, "zone_code") and _has_col(conn, "zone", "zone_code"):
            join_zone = "p.zone_code = z.zone_code"
        else:
            return _csv_utf8_bom("proficiency_jp.csv", HEADERS_JP, [])

        # 表示用「区」列
        if _has_col(conn, "zone", "zone_name"):
            zone_disp = "z.zone_name as 区"
            order_zone = "z.zone_name"
        elif _has_col(conn, "zone", "zone_code"):
            zone_disp = "z.zone_code as 区"
            order_zone = "z.zone_code"
        else:
            zone_disp = "cast(z.zone_id as text) as 区"
            order_zone = "z.zone_id"

        order_emp = "e.employee_code" if _has_col(conn, "employee", "employee_code") else "e.employee_id"

        sql = f"""
            select e.employee_code as 社員番号,
                   {zone_disp},
                   {priority_expr()},
                   {rv_expr}
              from {prof_table} p
              join employee e on {join_emp}
              join zone     z on {join_zone}
             where e.team_id = :tid and z.team_id = :tid
             order by {order_emp}, {order_zone}
        """
        rows = conn.execute(text(sql), {"tid": team_id}).all()
        data = [list(r) for r in rows]

    return _csv_utf8_bom("proficiency_jp.csv", HEADERS_JP, data)
    
# Excelの「更新」ボタンから送られてくる xlsm/xlsx を取り込み（既存 CLI を呼ぶ）
@app.post("/import")
async def import_xlsx(req: Request):
    content = await req.body()
    if not content:
        raise HTTPException(400, "empty body")
    # ↓ ここを .xlsm に
    with tempfile.NamedTemporaryFile(suffix=".xlsm", delete=False) as f:
        f.write(content)
        tmp = f.name
    try:
        cmd = [sys.executable, "-m", "posms.cli", "import-excel", "--file", tmp]
        cp = subprocess.run(cmd, capture_output=True, text=True)
        if cp.returncode == 0:
            return JSONResponse({"ok": True})
        return JSONResponse({"ok": False, "stderr": cp.stderr.strip(), "stdout": cp.stdout.strip()}, status_code=500)
    finally:
        try:
            os.unlink(tmp)
        except Exception:
            pass

# ローカル開発用: `python -m posms.api.main`
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("posms.api.main:app", host="127.0.0.1", port=8000, reload=True)
