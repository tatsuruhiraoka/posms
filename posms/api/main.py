# posms/api/main.py
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, JSONResponse
from sqlalchemy import create_engine, text
from typing import List, Optional
from fastapi import Query
from fastapi import Response
from starlette.middleware.gzip import GZipMiddleware
import os, io, csv, datetime as dt

app.add_middleware(GZipMiddleware, minimum_size=1024)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://posms@db:5432/posms")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="posms-api")

@app.get("/health")
def health():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"ok": True}

# --- DB → CSV（祝日） ---
@app.get("/export/holidays.csv", response_class=PlainTextResponse)
def export_holidays_csv():
    sql = """
      SELECT holiday_date::date
      FROM holidays
      WHERE is_holiday = TRUE
      ORDER BY holiday_date
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["holiday_date"])
    for (d,) in rows:
        w.writerow([d.strftime("%Y-%m-%d")])
    csv_data = buf.getvalue()
    csv_bytes = ("\ufeff" + csv_data).encode("utf-16le")
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-16",
    )

# --- CSV → DB（祝日 UPSERT） ---
@app.post("/import/holidays.csv")
async def import_holidays_csv(req: Request):
    # Content-Type は問わず、素のCSV本文を受け付け
    content = (await req.body()).decode("utf-8-sig", "ignore")
    rdr = csv.DictReader(io.StringIO(content))
    with engine.begin() as conn:
        for row in rdr:
            if not row.get("holiday_date"):
                continue
            d = dt.date.fromisoformat(row["holiday_date"].strip())
            conn.execute(text("""
                INSERT INTO holidays (holiday_date, is_holiday)
                VALUES (:d, TRUE)
                ON CONFLICT (holiday_date) DO UPDATE
                SET is_holiday = EXCLUDED.is_holiday
            """), {"d": d})
    return JSONResponse({"status": "ok", "upserted": "holidays"})
    
# --- 班の一覧（部署名付き）を CSV で返す ---
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
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["team_id","department_id","department_name","team_name"])
    for r in rows:
        w.writerow(r)
    csv_data = buf.getvalue()
    csv_bytes = ("\ufeff" + csv_data).encode("utf-16le")
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-16",
    )

# --- 指定班の社員を CSV で返す（氏名・役職など） ---
@app.get("/export/employees.csv", response_class=PlainTextResponse)
def export_employees_csv(
    team_id: int | None = None,
    order: str = "id",
    fields: Optional[str] = None,  # 追加: 必要列だけ返す
):
    # ホワイトリスト（安全な列のみ）
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
        # zone_name はマッピングがある場合のみ
        "zone_name": "z.zone_name",
    }

    # 選択された列（未指定なら従来どおりの主要列）
    if fields:
        req = [s.strip() for s in fields.split(",") if s.strip()]
        sel_cols = [c for c in req if c in colmap]
        if not sel_cols:
            sel_cols = ["employee_id", "employee_code", "name", "position", "team_id", "team_name", "department_name"]
    else:
        sel_cols = ["employee_id", "employee_code", "name", "position", "team_id", "team_name", "department_name"]

    # 必要に応じて JOIN を足す
    need_zone = "zone_name" in sel_cols

    base = """
      SELECT {select_list}
      FROM employee e
      JOIN team t        ON e.team_id = t.team_id
      JOIN department d  ON t.department_id = d.department_id
    """
    if need_zone:
        # 例: 中間テーブル employee_zone_map(employee_id, zone_id) がある場合
        base += """
        LEFT JOIN employee_zone_map ez ON ez.employee_id = e.employee_id
        LEFT JOIN zone z ON z.zone_id = ez.zone_id
        """

    # WHERE
    params = {}
    if team_id is not None:
        base += " WHERE e.team_id = :team_id"
        params["team_id"] = team_id

    # ORDER
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

    # SELECT リストを組み立て
    select_list = ", ".join(colmap[c] + f' AS "{c}"' for c in sel_cols)
    sql = base.format(select_list=select_list)

    # 実行
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    # CSV 出力（UTF-16LE + BOM）
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(sel_cols)
    for row in rows:
        # row は選択列の順で返るので、そのまま書き出し
        w.writerow([row[i] if row[i] is not None else "" for i in range(len(sel_cols))])

    csv_bytes = ("\ufeff" + buf.getvalue()).encode("utf-16le")
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-16")


# ローカル開発用（コンテナ外で python -m posms.api.main でも起動可）
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("posms.api.main:app", host="0.0.0.0", port=8000, reload=True)
