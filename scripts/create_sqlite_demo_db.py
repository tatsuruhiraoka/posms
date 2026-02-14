from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd


SCHEMA_SQL = r"""
PRAGMA foreign_keys=ON;

-- 1. マスタ系テーブル
CREATE TABLE IF NOT EXISTS office (
  office_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  office_name TEXT NOT NULL,
  office_code TEXT UNIQUE,
  created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
  is_active   INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS department (
  department_id INTEGER PRIMARY KEY AUTOINCREMENT,
  office_id     INTEGER NOT NULL REFERENCES office(office_id),
  department_name TEXT NOT NULL,
  department_code TEXT UNIQUE,
  created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at    TEXT DEFAULT CURRENT_TIMESTAMP,
  is_active     INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS team (
  team_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  department_id INTEGER NOT NULL REFERENCES department(department_id),
  team_name    TEXT NOT NULL,
  created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at   TEXT DEFAULT CURRENT_TIMESTAMP,
  is_active    INTEGER DEFAULT 1,
  CONSTRAINT team_unique_per_department UNIQUE (department_id, team_name)
);
CREATE INDEX IF NOT EXISTS idx_team_department ON team(department_id);

CREATE TABLE IF NOT EXISTS zone (
  zone_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  team_id      INTEGER NOT NULL REFERENCES team(team_id),
  zone_name    TEXT NOT NULL,
  operational_status TEXT NOT NULL,
  shift_type   TEXT NOT NULL DEFAULT '日勤',
  zone_code    TEXT UNIQUE,
  display_order INTEGER NOT NULL DEFAULT 0,     -- ★追加（CSV行順）
  created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at   TEXT DEFAULT CURRENT_TIMESTAMP,
  is_active    INTEGER DEFAULT 1,
  CONSTRAINT zone_unique_per_team UNIQUE (team_id, zone_name)
);
CREATE INDEX IF NOT EXISTS idx_zone_team ON zone(team_id);

CREATE TABLE IF NOT EXISTS jobtype (
  job_type_id  INTEGER PRIMARY KEY AUTOINCREMENT,
  classification TEXT NOT NULL,
  job_code     TEXT NOT NULL UNIQUE,
  job_name     TEXT NOT NULL,
  start_time   TEXT NOT NULL,
  end_time     TEXT NOT NULL,
  work_hours   INTEGER NOT NULL,
  display_order INTEGER NOT NULL DEFAULT 0,     -- ★追加（CSV行順）
  created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at   TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS leavetype (
  leave_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
  leave_code   TEXT NOT NULL UNIQUE,
  leave_name   TEXT NOT NULL,
  leave_category TEXT NOT NULL,
  created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at   TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS special_attendance_type (
  special_attendance_id INTEGER PRIMARY KEY AUTOINCREMENT,
  attendance_code TEXT UNIQUE NOT NULL,
  attendance_name TEXT NOT NULL,
  holiday_work_flag INTEGER NOT NULL DEFAULT 0,
  is_active  INTEGER NOT NULL DEFAULT 1,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- 2. 基本エンティティ
CREATE TABLE IF NOT EXISTS employee (
  employee_id  INTEGER PRIMARY KEY AUTOINCREMENT,
  employee_code TEXT NOT NULL UNIQUE,
  name         TEXT NOT NULL,
  employment_type TEXT NOT NULL,
  position     TEXT,
  default_work_hours INTEGER NOT NULL,
  monthly_work_hours INTEGER NOT NULL,
  team_id      INTEGER NOT NULL REFERENCES team(team_id),
  is_leader    INTEGER NOT NULL DEFAULT 0,
  is_vice_leader INTEGER NOT NULL DEFAULT 0,
  is_certifier INTEGER NOT NULL DEFAULT 0,
  display_order INTEGER NOT NULL DEFAULT 0,     -- ★追加（CSV行順）
  created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at   TEXT DEFAULT CURRENT_TIMESTAMP,
  CHECK (default_work_hours BETWEEN 1 AND 24),
  CHECK (monthly_work_hours BETWEEN 1 AND 300),
  CHECK (is_leader + is_vice_leader <= 1)
);

-- 3. 供給データ
CREATE TABLE IF NOT EXISTS mailvolume_by_type (
  date TEXT NOT NULL,
  office_id INTEGER NOT NULL REFERENCES office(office_id),
  mail_kind TEXT NOT NULL,
  actual_volume INTEGER,
  forecast_volume INTEGER,
  price_increase_flag INTEGER NOT NULL DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (date, office_id, mail_kind),
  CHECK (actual_volume   IS NULL OR actual_volume   >= 0),
  CHECK (forecast_volume IS NULL OR forecast_volume >= 0)
);
CREATE INDEX IF NOT EXISTS idx_mailvolume_by_type_office_date ON mailvolume_by_type(office_id, date);
CREATE INDEX IF NOT EXISTS idx_mailvolume_by_type_kind ON mailvolume_by_type(mail_kind);

CREATE TABLE IF NOT EXISTS demandprofile (
  zone_id INTEGER NOT NULL REFERENCES zone(zone_id),
  demand_mon INTEGER NOT NULL,
  demand_tue INTEGER NOT NULL,
  demand_wed INTEGER NOT NULL,
  demand_thu INTEGER NOT NULL,
  demand_fri INTEGER NOT NULL,
  demand_sat INTEGER NOT NULL,
  demand_sun INTEGER NOT NULL,
  demand_holiday INTEGER NOT NULL,
  PRIMARY KEY (zone_id),
  CHECK (demand_mon BETWEEN 0 AND 10),
  CHECK (demand_tue BETWEEN 0 AND 10),
  CHECK (demand_wed BETWEEN 0 AND 10),
  CHECK (demand_thu BETWEEN 0 AND 10),
  CHECK (demand_fri BETWEEN 0 AND 10),
  CHECK (demand_sat BETWEEN 0 AND 10),
  CHECK (demand_sun BETWEEN 0 AND 10),
  CHECK (demand_holiday BETWEEN 0 AND 10)
);

-- 4. 可否・例外管理系
CREATE TABLE IF NOT EXISTS employeezoneproficiency (
  employee_id INTEGER NOT NULL REFERENCES employee(employee_id),
  zone_id     INTEGER NOT NULL REFERENCES zone(zone_id),
  proficiency INTEGER NOT NULL,
  updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (employee_id, zone_id),
  CHECK (proficiency BETWEEN 0 AND 5)
);

-- ★ここを修正：shift種別は available_* に統一（文字列）
CREATE TABLE IF NOT EXISTS employee_availabilities (
  employee_id INTEGER PRIMARY KEY REFERENCES employee(employee_id),
  available_mon INTEGER NOT NULL DEFAULT 1,
  available_tue INTEGER NOT NULL DEFAULT 1,
  available_wed INTEGER NOT NULL DEFAULT 1,
  available_thu INTEGER NOT NULL DEFAULT 1,
  available_fri INTEGER NOT NULL DEFAULT 1,
  available_sat INTEGER NOT NULL DEFAULT 0,
  available_sun INTEGER NOT NULL DEFAULT 0,
  available_hol INTEGER NOT NULL DEFAULT 0,

  available_early     TEXT NOT NULL,
  available_day       TEXT NOT NULL,
  available_mid       TEXT NOT NULL,
  available_night     TEXT NOT NULL,
  available_night_sat TEXT NOT NULL,
  available_night_sun TEXT NOT NULL,
  available_night_hol TEXT NOT NULL,

  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS holiday (
  holiday_date TEXT PRIMARY KEY,
  name TEXT NOT NULL
);

-- 5. シフトアサイン
CREATE TABLE IF NOT EXISTS shiftassignment (
  employee_id INTEGER NOT NULL REFERENCES employee(employee_id),
  work_date   TEXT NOT NULL,
  zone_id     INTEGER REFERENCES zone(zone_id),
  job_type_id INTEGER REFERENCES jobtype(job_type_id),
  leave_type_id INTEGER REFERENCES leavetype(leave_type_id),
  special_attendance_id INTEGER REFERENCES special_attendance_type(special_attendance_id),
  PRIMARY KEY (employee_id, work_date),
  CHECK (
    (leave_type_id IS NOT NULL AND job_type_id IS NULL AND zone_id IS NULL AND special_attendance_id IS NULL)
    OR
    (leave_type_id IS NULL AND job_type_id IS NOT NULL AND zone_id IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_shift_date ON shiftassignment(work_date);
CREATE INDEX IF NOT EXISTS idx_shift_zone_date ON shiftassignment(zone_id, work_date);
CREATE INDEX IF NOT EXISTS idx_shift_job_date ON shiftassignment(job_type_id, work_date);
"""

DEPT_NAME_MAP = {
    "DPT-A": "第一集配営業部",
    "DPT-B": "第二集配営業部",
}


def q1(con: sqlite3.Connection, sql: str, params=()):
    row = con.execute(sql, params).fetchone()
    return row[0] if row else None


def read_csv_flexible(path: Path) -> pd.DataFrame:
    """
    CSVを柔軟に読み込む:
      - UTF-8 (BOMあり/なし) : utf-8-sig
      - Windows Excel由来の Shift JIS (CP932) : cp932 フォールバック
    """
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp932")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def to_int(v, default: int = 0) -> int:
    """
    Excel/CSV由来の値を安全に int 化する。
    - None, NaN, '' は default
    - '10.0' のような文字列もOK
    """
    if v is None:
        return default
    try:
        if isinstance(v, float) and pd.isna(v):
            return default
    except Exception:
        pass
    s = str(v).strip()
    if s == "":
        return default
    try:
        return int(float(s))
    except Exception:
        return default


def norm_date(v) -> str | None:
    """
    日付を YYYY-MM-DD に正規化する（SQLite内の文字列として統一）。
    """
    if v is None:
        return None
    try:
        if isinstance(v, float) and pd.isna(v):
            return None
    except Exception:
        pass
    dt = pd.to_datetime(v, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def ensure_office(con: sqlite3.Connection, office_code: str, office_name: str) -> int:
    oid = q1(con, "SELECT office_id FROM office WHERE office_code=?", (office_code,))
    if oid:
        return int(oid)
    con.execute(
        "INSERT INTO office (office_code, office_name) VALUES (?, ?)",
        (office_code, office_name),
    )
    return int(q1(con, "SELECT office_id FROM office WHERE office_code=?", (office_code,)))


def ensure_department(con: sqlite3.Connection, office_id: int, dc: str) -> int:
    dc = str(dc).strip()
    did = q1(con, "SELECT department_id FROM department WHERE department_code=?", (dc,))
    if did:
        return int(did)

    name = DEPT_NAME_MAP.get(dc, dc)  # ★ここで部名を決める
    con.execute(
        "INSERT INTO department (office_id, department_code, department_name) VALUES (?, ?, ?)",
        (int(office_id), dc, name),
    )
    return int(q1(con, "SELECT department_id FROM department WHERE department_code=?", (dc,)))


def ensure_team(con: sqlite3.Connection, department_id: int, tn: str) -> int:
    tn = str(tn).strip()
    tid = q1(
        con,
        "SELECT team_id FROM team WHERE department_id=? AND team_name=?",
        (department_id, tn),
    )
    if tid:
        return int(tid)
    con.execute(
        "INSERT INTO team (department_id, team_name) VALUES (?, ?)",
        (department_id, tn),
    )
    return int(q1(con, "SELECT team_id FROM team WHERE department_id=? AND team_name=?", (department_id, tn)))


def upsert_zone(
    con: sqlite3.Connection,
    team_id: int,
    zone_name: str,
    op: str,
    shift_type: str,
    display_order: int,
):
    con.execute(
        """
        INSERT INTO zone (team_id, zone_name, operational_status, shift_type, is_active, display_order)
        VALUES (?, ?, ?, ?, 1, ?)
        ON CONFLICT(team_id, zone_name) DO UPDATE SET
          operational_status=excluded.operational_status,
          shift_type=excluded.shift_type,
          is_active=1,
          display_order=excluded.display_order,
          updated_at=CURRENT_TIMESTAMP
        """,
        (int(team_id), zone_name, op, shift_type, int(display_order)),
    )


def upsert_demand(con: sqlite3.Connection, zone_id: int, r: dict):
    con.execute(
        """
        INSERT INTO demandprofile
          (zone_id, demand_mon, demand_tue, demand_wed, demand_thu, demand_fri, demand_sat, demand_sun, demand_holiday)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(zone_id) DO UPDATE SET
          demand_mon=excluded.demand_mon,
          demand_tue=excluded.demand_tue,
          demand_wed=excluded.demand_wed,
          demand_thu=excluded.demand_thu,
          demand_fri=excluded.demand_fri,
          demand_sat=excluded.demand_sat,
          demand_sun=excluded.demand_sun,
          demand_holiday=excluded.demand_holiday
        """,
        (
            int(zone_id),
            to_int(r.get("demand_mon"), 0),
            to_int(r.get("demand_tue"), 0),
            to_int(r.get("demand_wed"), 0),
            to_int(r.get("demand_thu"), 0),
            to_int(r.get("demand_fri"), 0),
            to_int(r.get("demand_sat"), 0),
            to_int(r.get("demand_sun"), 0),
            to_int(r.get("demand_holiday"), 0),
        ),
    )


def upsert_employee(con: sqlite3.Connection, r: dict, team_id: int, display_order: int):
    def i01(v, default=0) -> int:
        if v is None:
            return default
        try:
            if isinstance(v, float) and pd.isna(v):
                return default
        except Exception:
            pass
        s = str(v).strip()
        if s == "":
            return default
        if s.lower() in ("true", "t", "yes", "y", "on", "○", "◯"):
            return 1
        if s.lower() in ("false", "f", "no", "n", "off", "×"):
            return 0
        try:
            return 1 if int(float(s)) != 0 else 0
        except Exception:
            return default

    con.execute(
        """
        INSERT INTO employee
          (employee_code, name, employment_type, position,
           default_work_hours, monthly_work_hours,
           team_id, is_leader, is_vice_leader, is_certifier,
           display_order)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(employee_code) DO UPDATE SET
          name=excluded.name,
          employment_type=excluded.employment_type,
          position=excluded.position,
          default_work_hours=excluded.default_work_hours,
          monthly_work_hours=excluded.monthly_work_hours,
          team_id=excluded.team_id,
          is_leader=excluded.is_leader,
          is_vice_leader=excluded.is_vice_leader,
          is_certifier=excluded.is_certifier,
          display_order=excluded.display_order,
          updated_at=CURRENT_TIMESTAMP
        """,
        (
            str(r.get("employee_code") or "").strip(),
            str(r.get("name") or ""),
            str(r.get("employment_type") or ""),
            (str(r.get("position")).strip() if pd.notna(r.get("position")) else None),
            to_int(r.get("default_work_hours"), 0),
            to_int(r.get("monthly_work_hours"), 0),
            int(team_id),
            i01(r.get("is_leader"), 0),
            i01(r.get("is_vice_leader"), 0),
            i01(r.get("is_certifier"), 0),
            int(display_order),
        ),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csvdir", default="db/init/csv")
    ap.add_argument("--out", default="excel_templates/posms_demo.db")
    ap.add_argument("--office-code", default="HQ")
    ap.add_argument("--office-name", default="HQ")
    ap.add_argument("--default-zone-status", default="通配")
    ap.add_argument("--default-shift-type", default="日勤")
    ap.add_argument("--holiday-csv", default="holidays_jp_2020_2050.csv")
    ap.add_argument("--overwrite", action="store_true", help="delete existing db file")
    args = ap.parse_args()

    csvdir = Path(args.csvdir)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    if args.overwrite and out.exists():
        out.unlink()

    con = sqlite3.connect(out)
    con.execute("PRAGMA foreign_keys=ON;")
    con.executescript(SCHEMA_SQL)

    office_id = ensure_office(con, args.office_code, args.office_name)

    # jobtypes.csv（CSV行順を display_order に入れる）
    jobtypes = csvdir / "jobtypes.csv"
    if jobtypes.exists():
        df = normalize_columns(read_csv_flexible(jobtypes))
        for i, (_, r) in enumerate(df.iterrows(), start=1):
            con.execute(
                """
                INSERT INTO jobtype (job_code, classification, job_name, start_time, end_time, work_hours, display_order)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_code) DO UPDATE SET
                  classification=excluded.classification,
                  job_name=excluded.job_name,
                  start_time=excluded.start_time,
                  end_time=excluded.end_time,
                  work_hours=excluded.work_hours,
                  display_order=excluded.display_order,
                  updated_at=CURRENT_TIMESTAMP
                """,
                (
                    str(r.get("job_code") or "").strip(),
                    str(r.get("classification") or "").strip(),
                    str(r.get("job_name") or "").strip(),
                    str(r.get("start_time") or "").strip(),
                    str(r.get("end_time") or "").strip(),
                    to_int(r.get("work_hours"), 0),
                    int(i),
                ),
            )

    # zones.csv（CSV行順を zone.display_order に入れる）
    zones = csvdir / "zones.csv"
    if zones.exists():
        df = normalize_columns(read_csv_flexible(zones))
        for i, (_, r) in enumerate(df.iterrows(), start=1):
            dc = str(r.get("department_code") or "").strip()
            tn = str(r.get("team_name") or "").strip()
            zn = str(r.get("zone_name") or "").strip()
            if not (dc and tn and zn):
                continue
            did = ensure_department(con, office_id, dc)
            tid = ensure_team(con, did, tn)
            op = str(r.get("operational_status") or "").strip() or args.default_zone_status
            st = str(r.get("shift_type") or "").strip() or args.default_shift_type
            upsert_zone(con, tid, zn, op, st, i)

    # demand_profiles.csv（順序は不要）
    dp = csvdir / "demand_profiles.csv"
    if dp.exists():
        df = normalize_columns(read_csv_flexible(dp))
        for _, r in df.iterrows():
            dc = str(r.get("department_code") or "").strip()
            tn = str(r.get("team_name") or "").strip()
            zn = str(r.get("zone_name") or "").strip()
            if not (dc and tn and zn):
                continue
            did = ensure_department(con, office_id, dc)
            tid = ensure_team(con, did, tn)
            zid = q1(con, "SELECT zone_id FROM zone WHERE team_id=? AND zone_name=?", (tid, zn))
            if zid is None:
                continue
            upsert_demand(con, int(zid), dict(r))

    # employees.csv（CSV行順を employee.display_order に入れる）
    employees = csvdir / "employees.csv"
    if employees.exists():
        df = normalize_columns(read_csv_flexible(employees))
        for i, (_, r) in enumerate(df.iterrows(), start=1):
            dc = str(r.get("department_code") or "").strip()
            tn = str(r.get("team_name") or "").strip()
            if not (dc and tn):
                continue
            did = ensure_department(con, office_id, dc)
            tid = ensure_team(con, did, tn)
            upsert_employee(con, dict(r), tid, i)

    # employee_zone_proficiencies.csv（5列版）
    ezp = csvdir / "employee_zone_proficiencies.csv"
    if ezp.exists():
        df = normalize_columns(read_csv_flexible(ezp))
        for _, r in df.iterrows():
            ec = str(r.get("employee_code") or "").strip()
            dc = str(r.get("department_code") or "").strip()
            tn = str(r.get("team_name") or "").strip()
            zn = str(r.get("zone_name") or "").strip()
            prof = to_int(r.get("proficiency"), 0)
            if not (ec and dc and tn and zn):
                continue
            eid = q1(con, "SELECT employee_id FROM employee WHERE employee_code=?", (ec,))
            did = ensure_department(con, office_id, dc)
            tid = ensure_team(con, did, tn)
            zid = q1(con, "SELECT zone_id FROM zone WHERE team_id=? AND zone_name=?", (tid, zn))
            if eid is None or zid is None:
                continue
            con.execute(
                """
                INSERT INTO employeezoneproficiency (employee_id, zone_id, proficiency)
                VALUES (?, ?, ?)
                ON CONFLICT(employee_id, zone_id) DO UPDATE SET
                  proficiency=excluded.proficiency,
                  updated_at=CURRENT_TIMESTAMP
                """,
                (int(eid), int(zid), int(prof)),
            )

    # employee_availabilities.csv（曜日 + シフト種別）
    eav = csvdir / "employee_availabilities.csv"
    if eav.exists():
        df = normalize_columns(read_csv_flexible(eav))
        for _, r in df.iterrows():
            ec = str(r.get("employee_code") or "").strip()
            if not ec:
                continue
            eid = q1(con, "SELECT employee_id FROM employee WHERE employee_code=?", (ec,))
            if eid is None:
                continue
            con.execute(
                """
                INSERT INTO employee_availabilities
                  (employee_id,
                   available_mon, available_tue, available_wed, available_thu, available_fri,
                   available_sat, available_sun, available_hol,
                   available_early, available_day, available_mid,
                   available_night, available_night_sat,
                   available_night_sun, available_night_hol)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(employee_id) DO UPDATE SET
                  available_mon = excluded.available_mon,
                  available_tue = excluded.available_tue,
                  available_wed = excluded.available_wed,
                  available_thu = excluded.available_thu,
                  available_fri = excluded.available_fri,
                  available_sat = excluded.available_sat,
                  available_sun = excluded.available_sun,
                  available_hol = excluded.available_hol,
                  available_early     = excluded.available_early,
                  available_day       = excluded.available_day,
                  available_mid       = excluded.available_mid,
                  available_night     = excluded.available_night,
                  available_night_sat = excluded.available_night_sat,
                  available_night_sun = excluded.available_night_sun,
                  available_night_hol = excluded.available_night_hol,
                  updated_at = CURRENT_TIMESTAMP
                """,
                (
                    int(eid),
                    to_int(r.get("available_mon"), 1),
                    to_int(r.get("available_tue"), 1),
                    to_int(r.get("available_wed"), 1),
                    to_int(r.get("available_thu"), 1),
                    to_int(r.get("available_fri"), 1),
                    to_int(r.get("available_sat"), 0),
                    to_int(r.get("available_sun"), 0),
                    to_int(r.get("available_hol"), 0),
                    str(r.get("available_early") or "").strip(),
                    str(r.get("available_day") or "").strip(),
                    str(r.get("available_mid") or "").strip(),
                    str(r.get("available_night") or "").strip(),
                    str(r.get("available_night_sat") or "").strip(),
                    str(r.get("available_night_sun") or "").strip(),
                    str(r.get("available_night_hol") or "").strip(),
                ),
            )

    # holiday（holiday_dateを正規化）
    hpath = csvdir / args.holiday_csv
    if hpath.exists():
        df = normalize_columns(read_csv_flexible(hpath))
        for _, r in df.iterrows():
            d = norm_date(r.get("holiday_date"))
            name = str(r.get("name") or "").strip()
            if not (d and name):
                continue
            con.execute(
                """
                INSERT INTO holiday (holiday_date, name)
                VALUES (?, ?)
                ON CONFLICT(holiday_date) DO UPDATE SET name=excluded.name
                """,
                (d, name),
            )

    # postal_datas.csv → mailvolume_by_type
    postal = csvdir / "postal_datas.csv"
    if postal.exists():
        df = normalize_columns(read_csv_flexible(postal))

        required = ["date", "normal", "registered", "lp_plus", "nenga_assembly", "nenga_delivery"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"postal_datas.csv に必要列がありません: {missing}")

        kinds = ["normal", "registered", "lp_plus", "nenga_assembly", "nenga_delivery"]

        for _, r in df.iterrows():
            d = norm_date(r.get("date"))
            if not d:
                continue

            for kind in kinds:
                v = r.get(kind)

                # None/NaN/空文字はスキップ（“値がある列だけ入れる”）
                if v is None:
                    continue
                if isinstance(v, float) and pd.isna(v):
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue

                con.execute(
                    """
                    INSERT INTO mailvolume_by_type
                      (date, office_id, mail_kind, actual_volume, forecast_volume, price_increase_flag)
                    VALUES (?, ?, ?, ?, NULL, 0)
                    ON CONFLICT(date, office_id, mail_kind) DO UPDATE SET
                      actual_volume=excluded.actual_volume,
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    (d, office_id, kind, to_int(v, 0)),
                )
    con.commit()
    con.close()
    print(f"OK: created sqlite demo db -> {out}")


if __name__ == "__main__":
    main()
