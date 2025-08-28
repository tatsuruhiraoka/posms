\set ON_ERROR_STOP on
-- 10_seed_masters.sql

BEGIN;

-- ========= Office =========
INSERT INTO Office (office_name, office_code, is_active, updated_at)
VALUES ('局', 'HQ', TRUE, NOW())
ON CONFLICT (office_code) DO UPDATE
SET office_name = EXCLUDED.office_name,
    is_active   = EXCLUDED.is_active,
    updated_at  = NOW();

-- ========= Department =========
INSERT INTO Department (office_id, department_name, department_code, is_active, updated_at)
VALUES
  ((SELECT office_id FROM Office WHERE office_code = 'HQ'), '第一集配営業部', 'DPT-A', TRUE, NOW()),
  ((SELECT office_id FROM Office WHERE office_code = 'HQ'), '第二集配営業部', 'DPT-B', TRUE, NOW())
ON CONFLICT (department_code) DO UPDATE
SET department_name = EXCLUDED.department_name,
    office_id       = EXCLUDED.office_id,
    is_active       = EXCLUDED.is_active,
    updated_at      = NOW();

-- ========= Team =========
-- ※ (department_id, team_name) に UNIQUE がある前提（前の提案どおり）
WITH d1 AS (
  SELECT department_id FROM Department WHERE department_code = 'DPT-A'
),
d2 AS (
  SELECT department_id FROM Department WHERE department_code = 'DPT-B'
)
INSERT INTO Team (department_id, team_name, is_active, updated_at)
SELECT d1.department_id, t, TRUE, NOW() FROM d1, UNNEST(ARRAY['1班','2班','3班','4班','5班']) AS t
UNION ALL
SELECT d2.department_id, t, TRUE, NOW() FROM d2, UNNEST(ARRAY['6班','7班','8班','9班']) AS t
ON CONFLICT (department_id, team_name) DO UPDATE
SET is_active  = EXCLUDED.is_active,
    updated_at = NOW();

-- ========= LeaveType =========
INSERT INTO LeaveType (leave_code, leave_name, leave_category, updated_at) VALUES
  -- REGULAR
  ('OFF_DUTY',            '非番',   'REGULAR', NOW()),
  ('WEEKLY_OFF',          '週休',   'REGULAR', NOW()),
  ('PUBLIC_HOLIDAY_OFF',  '祝休',   'REGULAR', NOW()),
  -- SPECIAL
  ('PLANNED_ANNUAL_LEAVE','計年',   'SPECIAL', NOW()),
  ('ANNUAL_LEAVE',        '年休',   'SPECIAL', NOW()),
  ('SUMMER_LEAVE',        '夏期',   'SPECIAL', NOW()),
  ('WINTER_LEAVE',        '冬期',   'SPECIAL', NOW()),
  ('COMPENSATORY_LEAVE',  '代休',   'SPECIAL', NOW()),
  ('APPROVED_ABSENCE',    '承欠',   'SPECIAL', NOW()),
  ('MATERNITY_LEAVE',     '産休',   'SPECIAL', NOW()),
  ('PARENTAL_LEAVE',      '育休',   'SPECIAL', NOW()),
  ('CAREGIVER_LEAVE',     '介護',   'SPECIAL', NOW()),
  ('SICK_LEAVE',          '病休',   'SPECIAL', NOW()),
  ('LEAVE_OF_ABSENCE',    '休職',   'SPECIAL', NOW()),
  ('OTHER',               'その他', 'SPECIAL', NOW())
ON CONFLICT (leave_code) DO UPDATE
SET leave_name     = EXCLUDED.leave_name,
    leave_category = EXCLUDED.leave_category,
    updated_at     = NOW();

-- ========= SpecialAttendanceType（廃休・マル超） =========
INSERT INTO SpecialAttendanceType
  (attendance_code, attendance_name, holiday_work_flag, is_active, updated_at)
VALUES
  ('HAIKYU',  '廃休',  FALSE, TRUE, NOW()),
  ('MARUCHO', 'マル超', FALSE, TRUE, NOW())
ON CONFLICT (attendance_code) DO UPDATE
SET attendance_name   = EXCLUDED.attendance_name,
    holiday_work_flag = EXCLUDED.holiday_work_flag,
    is_active         = EXCLUDED.is_active,
    updated_at        = NOW();

COMMIT;

-- zone_code 自動採番
CREATE SEQUENCE IF NOT EXISTS zone_code_seq;

CREATE OR REPLACE FUNCTION gen_zone_code()
RETURNS trigger AS $$
BEGIN
  IF NEW.zone_code IS NULL OR btrim(NEW.zone_code) = '' THEN
    NEW.zone_code := 'Z' || to_char(nextval('zone_code_seq'), 'FM000000');
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_zone_code ON Zone;
CREATE TRIGGER trg_zone_code
BEFORE INSERT ON Zone
FOR EACH ROW EXECUTE FUNCTION gen_zone_code();


-- （任意）確認クエリ
-- SELECT office_id, office_name, office_code FROM Office;
-- SELECT department_id, department_name, department_code FROM Department;
-- SELECT department_id, team_name FROM Team ORDER BY department_id, team_name;
-- SELECT job_code, job_name, start_time, end_time, work_hours, crosses_midnight FROM JobType;
-- SELECT leave_code, leave_name, leave_category FROM LeaveType;
-- SELECT attendance_code, attendance_name FROM SpecialAttendanceType;
