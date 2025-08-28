-- 1. マスタ系テーブル
--局情報
CREATE TABLE Office (
  office_id   SERIAL    PRIMARY KEY,
  office_name VARCHAR(100) NOT NULL,
  office_code VARCHAR(20) UNIQUE,
  created_at  TIMESTAMP DEFAULT NOW(),
  updated_at  TIMESTAMP DEFAULT NOW(),
  is_active   BOOLEAN   DEFAULT TRUE
);

--部
CREATE TABLE Department (
  department_id   SERIAL    PRIMARY KEY,
  office_id INT       NOT NULL
    REFERENCES Office(office_id),
  department_name VARCHAR(100) NOT NULL,
  department_code VARCHAR(20) UNIQUE,
  created_at      TIMESTAMP DEFAULT NOW(),
  updated_at      TIMESTAMP DEFAULT NOW(),
  is_active       BOOLEAN   DEFAULT TRUE
);

--班
CREATE TABLE Team (
  team_id       SERIAL PRIMARY KEY,
  department_id INT    NOT NULL REFERENCES Department(department_id),
  team_name     VARCHAR(100) NOT NULL,
  created_at    TIMESTAMP DEFAULT NOW(),
  updated_at    TIMESTAMP DEFAULT NOW(),
  is_active     BOOLEAN  DEFAULT TRUE,
  CONSTRAINT team_unique_per_department UNIQUE (department_id, team_name)
);

CREATE INDEX IF NOT EXISTS idx_team_department ON Team(department_id);

--区
CREATE TABLE Zone (
  zone_id            SERIAL       PRIMARY KEY,
  team_id            INT          NOT NULL REFERENCES Team(team_id),
  zone_name          VARCHAR(100) NOT NULL,
  operational_status VARCHAR(50)  NOT NULL,
  zone_code          VARCHAR(20)  UNIQUE,
  created_at         TIMESTAMP    DEFAULT NOW(),
  updated_at         TIMESTAMP    DEFAULT NOW(),
  is_active          BOOLEAN      DEFAULT TRUE,
  CONSTRAINT zone_unique_per_team UNIQUE (team_id, zone_name)
);

CREATE INDEX IF NOT EXISTS idx_zone_team ON Zone(team_id);

--時間帯
CREATE TABLE JobType (
  job_type_id   SERIAL    PRIMARY KEY,
  classification VARCHAR(20) NOT NULL,
  job_code      VARCHAR(20) NOT NULL UNIQUE,
  job_name      VARCHAR(50) NOT NULL,
  start_time    TIME      NOT NULL,
  end_time      TIME      NOT NULL,
  work_hours    INT       NOT NULL,
  crosses_midnight BOOLEAN     NOT NULL DEFAULT FALSE,-- 深夜勤(翌日跨ぎ)かどうか
  created_at    TIMESTAMP DEFAULT NOW(),
  updated_at    TIMESTAMP DEFAULT NOW()
);

--休暇種類
CREATE TABLE LeaveType (
  leave_type_id SERIAL    PRIMARY KEY,
  leave_code    VARCHAR(20) NOT NULL UNIQUE,
  leave_name    VARCHAR(50) NOT NULL,
  leave_category VARCHAR(10) NOT NULL,
  created_at    TIMESTAMP DEFAULT NOW(),
  updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE SpecialAttendanceType (
  special_attendance_id SERIAL      PRIMARY KEY,
  attendance_code       VARCHAR(20) UNIQUE NOT NULL,   -- 'HAIKYU', 'MARUCHO'
  attendance_name       VARCHAR(50) NOT NULL,          -- 廃休, マル超
  holiday_work_flag     BOOLEAN     NOT NULL DEFAULT FALSE,  -- 休日勤務か？
  is_active             BOOLEAN     NOT NULL DEFAULT TRUE,
  created_at            TIMESTAMP   DEFAULT NOW(),
  updated_at            TIMESTAMP   DEFAULT NOW()
);

-- 2. 基本エンティティ
CREATE TABLE Employee (
  employee_id           SERIAL       PRIMARY KEY,
  employee_code         VARCHAR(40)  NOT NULL UNIQUE,   -- ★ 追加：自然キー
  name                  VARCHAR(100) NOT NULL,
  employment_type       VARCHAR(10)  NOT NULL,
  position              VARCHAR(50),
  /* 所定時間 */
  default_work_hours    SMALLINT     NOT NULL CHECK (default_work_hours BETWEEN 1 AND 24),
  monthly_work_hours    SMALLINT     NOT NULL CHECK (monthly_work_hours BETWEEN 1 AND 300),
  team_id               INT          NOT NULL REFERENCES Team(team_id),
  is_certifier          BOOLEAN      NOT NULL DEFAULT FALSE, -- 認証司
  created_at            TIMESTAMP    DEFAULT NOW(),
  updated_at            TIMESTAMP    DEFAULT NOW()
);


-- 3. 供給データ（局全体の物数）
CREATE TABLE MailVolume (
  date                 DATE      NOT NULL,
  office_id            INT       NOT NULL REFERENCES Office(office_id),
  actual_volume        INT,                            -- 入力が無い日はNULL可
  forecast_volume      INT,                            -- 予測は後で更新するのでNULL可
  price_increase_flag  BOOLEAN   NOT NULL DEFAULT FALSE,
  created_at           TIMESTAMP DEFAULT NOW(),
  updated_at           TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (date, office_id),

  -- 値がある場合は非負に制約
  CONSTRAINT chk_mv_actual_nonneg   CHECK (actual_volume   IS NULL OR actual_volume   >= 0),
  CONSTRAINT chk_mv_forecast_nonneg CHECK (forecast_volume IS NULL OR forecast_volume >= 0)
);

-- 検索最適化（officeごとの時系列取得に有効）
CREATE INDEX  IF NOT EXISTS idx_mailvolume_office_date ON MailVolume(office_id, date);

--各区の曜日ごとの需要
CREATE TABLE DemandProfile (
  zone_id        INT    NOT NULL
    REFERENCES Zone(zone_id),
  demand_mon     INT    NOT NULL CHECK (demand_mon   BETWEEN 0 AND 10),
  demand_tue     INT    NOT NULL CHECK (demand_tue   BETWEEN 0 AND 10),
  demand_wed     INT    NOT NULL CHECK (demand_wed   BETWEEN 0 AND 10),
  demand_thu     INT    NOT NULL CHECK (demand_thu   BETWEEN 0 AND 10),
  demand_fri     INT    NOT NULL CHECK (demand_fri   BETWEEN 0 AND 10),
  demand_sat     INT    NOT NULL CHECK (demand_sat   BETWEEN 0 AND 10),
  demand_sun     INT    NOT NULL CHECK (demand_sun   BETWEEN 0 AND 10),
  demand_holiday INT    NOT NULL CHECK (demand_holiday BETWEEN 0 AND 10),
  PRIMARY KEY (zone_id)
);


-- 4. 可否・例外管理系
--各社員の区を担当する優先度
CREATE TABLE EmployeeZoneProficiency (
  employee_id  INT      NOT NULL
    REFERENCES Employee(employee_id),
  zone_id      INT      NOT NULL
    REFERENCES Zone(zone_id),
  proficiency  SMALLINT NOT NULL CHECK (proficiency BETWEEN 0 AND 5),
  updated_at   TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (employee_id, zone_id)
);

--各社員の出勤可能な曜日
CREATE TABLE EmployeeAvailability (
  employee_id    INT     PRIMARY KEY
    REFERENCES Employee(employee_id),
  available_mon  BOOLEAN NOT NULL DEFAULT TRUE,
  available_tue  BOOLEAN NOT NULL DEFAULT TRUE,
  available_wed  BOOLEAN NOT NULL DEFAULT TRUE,
  available_thu  BOOLEAN NOT NULL DEFAULT TRUE,
  available_fri  BOOLEAN NOT NULL DEFAULT TRUE,
  available_sat  BOOLEAN NOT NULL DEFAULT FALSE,
  available_sun  BOOLEAN NOT NULL DEFAULT FALSE,
  available_hol  BOOLEAN NOT NULL DEFAULT FALSE
);

-- 5. シフトアサイン
CREATE TABLE ShiftAssignment (
  employee_id     INT  NOT NULL REFERENCES Employee(employee_id),
  work_date       DATE NOT NULL,  -- シフトの日付
  zone_id         INT      NULL REFERENCES Zone(zone_id),-- 担当する区
  job_type_id     INT      NULL REFERENCES JobType(job_type_id),-- 勤務時間帯
  leave_type_id   INT      NULL REFERENCES LeaveType(leave_type_id),
  -- 特殊区分（廃休・マル超など）を単一で持つなら以下を残す。複数付く可能性があるなら別テーブル推奨
  special_attendance_id INT NULL REFERENCES SpecialAttendanceType(special_attendance_id),
  PRIMARY KEY (employee_id, work_date),

  -- 休みor勤務のどちらか一方だけを許す
  CHECK (
    (leave_type_id IS NOT NULL AND job_type_id IS NULL AND zone_id IS NULL
     AND special_attendance_id IS NULL)         -- 休み
    OR
    (leave_type_id IS NULL AND job_type_id IS NOT NULL AND zone_id IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_shift_date      ON ShiftAssignment(work_date);
CREATE INDEX IF NOT EXISTS idx_shift_zone_date ON ShiftAssignment(zone_id, work_date) WHERE job_type_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_shift_job_date  ON ShiftAssignment(job_type_id, work_date) WHERE job_type_id IS NOT NULL;