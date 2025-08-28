\set ON_ERROR_STOP on
\timing on

-- ローカル（Compose自動実行/手動実行） … デフォルトで /docker-entrypoint-initdb.d/csv/*.csv を読む
--CI（GitHub Actions） … psql 実行時に -v csvdir='db/init/csv' を渡して、リポジトリ内の CSV を読む
\if :{?csvdir} \else \set csvdir '/docker-entrypoint-initdb.d/csv' \endif
------------------------------------------------------------
-- 1) JobType  (jobtypes.csv)
--   job_code,classification,job_name,start_time,end_time,work_hours,crosses_midnight
------------------------------------------------------------
DROP TABLE IF EXISTS stage_jobtype;
CREATE TEMP TABLE stage_jobtype (
  classification    text,
  job_code          text,
  job_name          text,
  start_time        time,
  end_time          time,
  work_hours        int,
  crosses_midnight  boolean
);

COPY stage_jobtype
FROM '/docker-entrypoint-initdb.d/csv/jobtypes.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

BEGIN;
INSERT INTO JobType (job_code, classification, job_name, start_time, end_time, work_hours, crosses_midnight, updated_at)
SELECT job_code, classification, job_name, start_time, end_time, work_hours, COALESCE(crosses_midnight, FALSE), NOW()
FROM stage_jobtype
ON CONFLICT (job_code) DO UPDATE
SET classification    = EXCLUDED.classification,
    job_name         = EXCLUDED.job_name,
    start_time       = EXCLUDED.start_time,
    end_time         = EXCLUDED.end_time,
    work_hours       = EXCLUDED.work_hours,
    crosses_midnight = EXCLUDED.crosses_midnight,
    updated_at       = NOW();
COMMIT;

------------------------------------------------------------
-- 2) Zone  (zones.csv)  ※ zone_code はCSVに載せない（DBが自動採番）
--   department_code,team_name,zone_name,operational_status,is_active
------------------------------------------------------------
DROP TABLE IF EXISTS stage_zone;
CREATE TEMP TABLE stage_zone (
  department_code    text,
  team_name          text,
  zone_name          text,
  operational_status text,
  is_active          boolean
);

COPY stage_zone(department_code, team_name, zone_name, operational_status, is_active)
FROM '/docker-entrypoint-initdb.d/csv/zones.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

BEGIN;
INSERT INTO Zone (team_id, zone_name, operational_status, is_active, updated_at)
SELECT
  t.team_id,
  s.zone_name,
  COALESCE(NULLIF(s.operational_status,''), 'active'),
  COALESCE(s.is_active, TRUE),
  NOW()
FROM stage_zone s
JOIN Department d ON d.department_code = s.department_code
JOIN Team       t ON t.department_id   = d.department_id
                 AND t.team_name       = s.team_name
ON CONFLICT ON CONSTRAINT zone_unique_per_team
DO UPDATE SET
  operational_status = EXCLUDED.operational_status,
  is_active          = EXCLUDED.is_active,
  updated_at         = NOW();
COMMIT;

------------------------------------------------------------
-- 3) DemandProfile  (demand_profiles.csv)  ※ zone_code 不要
--   department_code,team_name,zone_name,demand_mon,...,demand_holiday
------------------------------------------------------------
DROP TABLE IF EXISTS stage_demand_profile;
CREATE TEMP TABLE stage_demand_profile (
  department_code text,
  team_name       text,
  zone_name       text,
  demand_mon      int,
  demand_tue      int,
  demand_wed      int,
  demand_thu      int,
  demand_fri      int,
  demand_sat      int,
  demand_sun      int,
  demand_holiday  int
);

COPY stage_demand_profile
FROM '/docker-entrypoint-initdb.d/csv/demand_profiles.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

BEGIN;
WITH team_resolved AS (
  SELECT s.*, d.department_id, t.team_id
  FROM stage_demand_profile s
  JOIN Department d ON d.department_code = s.department_code
  JOIN Team       t ON t.department_id   = d.department_id AND t.team_name = s.team_name
),
zone_resolved AS (
  SELECT tr.*, z.zone_id
  FROM team_resolved tr
  JOIN Zone z ON z.team_id = tr.team_id AND z.zone_name = tr.zone_name
)
INSERT INTO DemandProfile (zone_id, demand_mon, demand_tue, demand_wed, demand_thu,
                           demand_fri, demand_sat, demand_sun, demand_holiday)
SELECT zr.zone_id, zr.demand_mon, zr.demand_tue, zr.demand_wed, zr.demand_thu,
       zr.demand_fri, zr.demand_sat, zr.demand_sun, zr.demand_holiday
FROM zone_resolved zr
ON CONFLICT (zone_id) DO UPDATE
SET demand_mon      = EXCLUDED.demand_mon,
    demand_tue      = EXCLUDED.demand_tue,
    demand_wed      = EXCLUDED.demand_wed,
    demand_thu      = EXCLUDED.demand_thu,
    demand_fri      = EXCLUDED.demand_fri,
    demand_sat      = EXCLUDED.demand_sat,
    demand_sun      = EXCLUDED.demand_sun,
    demand_holiday  = EXCLUDED.demand_holiday;
COMMIT;

------------------------------------------------------------
-- 4) MailVolume  (mail_volumes.csv)
--   CSV想定: date,actual_volume だけ
--   オフィスは -v office_code=... で指定。未指定なら 'HQ'
------------------------------------------------------------
\if :{?office_code} \else \set office_code 'HQ' \endif

DROP TABLE IF EXISTS stage_mail_volume;
CREATE TEMP TABLE stage_mail_volume (
  date           date,
  actual_volume  int
);

-- 2列だけを列リスト指定で読み込む
COPY stage_mail_volume (date, actual_volume)
FROM '/docker-entrypoint-initdb.d/csv/mail_volumes.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

BEGIN;
INSERT INTO MailVolume (date, office_id, actual_volume, forecast_volume, price_increase_flag)
SELECT
  s.date,
  o.office_id,
  s.actual_volume,
  NULL,         -- 予測は後で埋める（当面はNULLのまま）
  FALSE         -- ハイパラ導入前は FALSE 固定
FROM stage_mail_volume s
JOIN Office o ON o.office_code = :'office_code'
ON CONFLICT (date, office_id) DO UPDATE
SET actual_volume       = EXCLUDED.actual_volume,
    forecast_volume     = EXCLUDED.forecast_volume,     -- ここはNULL上書きでOK（後から更新する前提）
    price_increase_flag = EXCLUDED.price_increase_flag;
COMMIT;

------------------------------------------------------------
-- 5) Employee  (employees.csv)
--   employee_code,name,employment_type,position,default_work_hours,monthly_work_hours,department_code,team_name,is_certifier?
--   ※ is_certifier をCSVに載せない場合、DEFAULT FALSEが効くように下で補完します
------------------------------------------------------------
DROP TABLE IF EXISTS stage_employees;
CREATE TEMP TABLE stage_employees (
  employee_code      text,
  name               text,
  employment_type    text,
  position           text,
  default_work_hours int,
  monthly_work_hours int,
  department_code    text,
  team_name          text,
  is_certifier       text
);

COPY stage_employees
FROM '/docker-entrypoint-initdb.d/csv/employees.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- （任意推奨）前検品：employee_code が空の行を弾く
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM stage_employees
    WHERE employee_code IS NULL OR btrim(employee_code) = ''
  ) THEN
    RAISE EXCEPTION 'employees.csv: employee_code is required (found blank).';
  END IF;
END $$;

BEGIN;
INSERT INTO Employee (
  employee_code, name, employment_type, position,
  default_work_hours, monthly_work_hours, team_id, is_certifier, updated_at
)
SELECT
  btrim(s.employee_code),
  s.name,
  s.employment_type,
  s.position,
  s.default_work_hours,
  s.monthly_work_hours,
  t.team_id,
  CASE
    WHEN upper(btrim(s.is_certifier)) IN ('TRUE','T','1','Y') THEN TRUE
    ELSE FALSE
  END,
  NOW()
FROM stage_employees s
JOIN Department d ON d.department_code = s.department_code
JOIN Team       t ON t.department_id   = d.department_id
                 AND t.team_name       = s.team_name
ON CONFLICT (employee_code) DO UPDATE
SET name               = EXCLUDED.name,
    employment_type    = EXCLUDED.employment_type,
    position           = EXCLUDED.position,
    default_work_hours = EXCLUDED.default_work_hours,
    monthly_work_hours = EXCLUDED.monthly_work_hours,
    team_id            = EXCLUDED.team_id,
    is_certifier       = EXCLUDED.is_certifier,
    updated_at         = NOW();
COMMIT;

------------------------------------------------------------
-- 6) EmployeeZoneProficiency  (employee_zone_proficiencys.csv)  ※ zone_code不要
--   employee_code,department_code,team_name,zone_name,proficiency
------------------------------------------------------------
-- 6) EmployeeZoneProficiency  (employee_zone_proficiencies.csv)  ※ zone_code不要
DROP TABLE IF EXISTS stage_ezp;
CREATE TEMP TABLE stage_ezp (
  employee_code   text,
  department_code text,
  team_name       text,
  zone_name       text,
  proficiency     int
);

COPY stage_ezp
FROM '/docker-entrypoint-initdb.d/csv/employee_zone_proficiencies.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

BEGIN;
WITH team_resolved AS (
  SELECT s.*, d.department_id, t.team_id
  FROM stage_ezp s
  JOIN Department d ON d.department_code = s.department_code
  JOIN Team t ON t.department_id = d.department_id AND t.team_name = s.team_name
),
zone_resolved AS (
  SELECT tr.*, z.zone_id
  FROM team_resolved tr
  JOIN Zone z ON z.team_id = tr.team_id AND z.zone_name = tr.zone_name
)
INSERT INTO EmployeeZoneProficiency (employee_id, zone_id, proficiency, updated_at)
SELECT e.employee_id,
       zr.zone_id,
       GREATEST(0, LEAST(5, COALESCE(zr.proficiency, 0))),
       NOW()
FROM zone_resolved zr
JOIN Employee e ON e.employee_code = zr.employee_code
ON CONFLICT (employee_id, zone_id) DO UPDATE
SET proficiency = EXCLUDED.proficiency,
    updated_at  = NOW();
COMMIT;


------------------------------------------------------------
-- 7) EmployeeAvailability  (employee_availabilities.csv)
--   employee_code,available_mon,...,available_hol
------------------------------------------------------------
DROP TABLE IF EXISTS stage_eavail;
CREATE TEMP TABLE stage_eavail (
  employee_code  text,
  available_mon  boolean,
  available_tue  boolean,
  available_wed  boolean,
  available_thu  boolean,
  available_fri  boolean,
  available_sat  boolean,
  available_sun  boolean,
  available_hol  boolean
);

COPY stage_eavail
FROM '/docker-entrypoint-initdb.d/csv/employee_availabilities.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

BEGIN;
INSERT INTO EmployeeAvailability (employee_id,
  available_mon, available_tue, available_wed, available_thu,
  available_fri, available_sat, available_sun, available_hol)
SELECT e.employee_id,
       COALESCE(s.available_mon, TRUE),
       COALESCE(s.available_tue, TRUE),
       COALESCE(s.available_wed, TRUE),
       COALESCE(s.available_thu, TRUE),
       COALESCE(s.available_fri, TRUE),
       COALESCE(s.available_sat, FALSE),
       COALESCE(s.available_sun, FALSE),
       COALESCE(s.available_hol, FALSE)
FROM stage_eavail s
JOIN Employee e ON e.employee_code = s.employee_code
ON CONFLICT (employee_id) DO UPDATE
SET available_mon = EXCLUDED.available_mon,
    available_tue = EXCLUDED.available_tue,
    available_wed = EXCLUDED.available_wed,
    available_thu = EXCLUDED.available_thu,
    available_fri = EXCLUDED.available_fri,
    available_sat = EXCLUDED.available_sat,
    available_sun = EXCLUDED.available_sun,
    available_hol = EXCLUDED.available_hol;
COMMIT;
