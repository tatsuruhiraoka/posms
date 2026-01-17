-- 1. マスタ系テーブル
-- 局情報
create table office (
  office_id   serial primary key,
  office_name varchar(100) not null,
  office_code varchar(20) unique,
  created_at  timestamp default now(),
  updated_at  timestamp default now(),
  is_active   int default 1
);

-- 部
create table department (
  department_id serial primary key,
  office_id int not null references office(office_id),
  department_name varchar(100) not null,
  department_code varchar(20) unique,
  created_at timestamp default now(),
  updated_at timestamp default now(),
  is_active int default 1
);

-- 班
create table team (
  team_id serial primary key,
  department_id int not null references department(department_id),
  team_name varchar(100) not null,
  created_at timestamp default now(),
  updated_at timestamp default now(),
  is_active int default 1,
  constraint team_unique_per_department unique (department_id, team_name)
);

create index if not exists idx_team_department on team(department_id);

-- 区
create table zone (
  zone_id serial primary key,
  team_id int not null references team(team_id),
  zone_name varchar(100) not null,
  operational_status varchar(50) not null,
  shift_type varchar(20) not null default '日勤',
  zone_code varchar(20) unique,
  created_at timestamp default now(),
  updated_at timestamp default now(),
  is_active int default 1,
  constraint zone_unique_per_team unique (team_id, zone_name)
);

create index if not exists idx_zone_team on zone(team_id);

-- 時間帯
create table jobtype (
  job_type_id serial primary key,
  classification varchar(20) not null,
  job_code varchar(20) not null unique,
  job_name varchar(50) not null,
  start_time time not null,
  end_time time not null,
  work_hours int not null,
  created_at timestamp default now(),
  updated_at timestamp default now()
);

-- 休暇種類
create table leavetype (
  leave_type_id serial primary key,
  leave_code varchar(20) not null unique,
  leave_name varchar(50) not null,
  leave_category varchar(10) not null,
  created_at timestamp default now(),
  updated_at timestamp default now()
);

-- 特殊区分（廃休・マル超など）
create table special_attendance_type (
  special_attendance_id serial primary key,
  attendance_code varchar(20) unique not null,   -- 'HAIKYU', 'MARUCHO'
  attendance_name varchar(50) not null,          -- 廃休, マル超
  holiday_work_flag int not null default 0,      -- 休日勤務か？
  is_active int not null default 1,
  created_at timestamp default now(),
  updated_at timestamp default now()
);

-- 2. 基本エンティティ
create table employee (
  employee_id serial primary key,
  employee_code varchar(40) not null unique,   -- 自然キー
  name varchar(100) not null,
  employment_type varchar(10) not null,
  position varchar(50),
  default_work_hours smallint not null check (default_work_hours between 1 and 24),
  monthly_work_hours smallint not null check (monthly_work_hours between 1 and 300),
  team_id int not null references team(team_id),
  is_leader int not null default 0,            -- 班長
  is_vice_leader int not null default 0,       -- 副班長
  is_certifier int not null default 0,         -- 認証司
  created_at timestamp default now(),
  updated_at timestamp default now(),
  check (is_leader + is_vice_leader <= 1)      -- 同一人が班長かつ副班長にならない
);

-- 3. 供給データ（局全体の物数：種別別）
create table mailvolume_by_type (
  date date not null,
  office_id int not null references office(office_id),
  mail_kind text not null,          -- normal / kakitome / yu_packet / ... を入れる
  actual_volume int,
  forecast_volume int,
  price_increase_flag int not null default 0,
  created_at timestamp default now(),
  updated_at timestamp default now(),
  primary key (date, office_id, mail_kind),
  constraint chk_mvt_actual_nonneg
    check (actual_volume   is null or actual_volume   >= 0),
  constraint chk_mvt_forecast_nonneg
    check (forecast_volume is null or forecast_volume >= 0)
);

create index if not exists idx_mailvolume_by_type_office_date
  on mailvolume_by_type(office_id, date);

create index if not exists idx_mailvolume_by_type_kind
  on mailvolume_by_type(mail_kind);

-- 各区の曜日ごとの需要
create table demandprofile (
  zone_id int not null references zone(zone_id),
  demand_mon int not null check (demand_mon between 0 and 10),
  demand_tue int not null check (demand_tue between 0 and 10),
  demand_wed int not null check (demand_wed between 0 and 10),
  demand_thu int not null check (demand_thu between 0 and 10),
  demand_fri int not null check (demand_fri between 0 and 10),
  demand_sat int not null check (demand_sat between 0 and 10),
  demand_sun int not null check (demand_sun between 0 and 10),
  demand_holiday int not null check (demand_holiday between 0 and 10),
  primary key (zone_id)
);

-- 4. 可否・例外管理系
-- 各社員の区を担当する優先度
create table employeezoneproficiency (
  employee_id int not null references employee(employee_id),
  zone_id int not null references zone(zone_id),
  proficiency smallint not null check (proficiency between 0 and 5),
  updated_at timestamp default now(),
  primary key (employee_id, zone_id)
);

-- 各社員の出勤可能な曜日
create table employeeavailability (
  employee_id int primary key references employee(employee_id),
  available_mon int not null default 1,
  available_tue int not null default 1,
  available_wed int not null default 1,
  available_thu int not null default 1,
  available_fri int not null default 1,
  available_sat int not null default 0,
  available_sun int not null default 0,
  available_hol int not null default 0
);

create table if not exists holiday (
  holiday_date date primary key,
  name text not null
);

create index if not exists idx_holiday_date on holiday(holiday_date);

-- 5. シフトアサイン
create table shiftassignment (
  employee_id int not null references employee(employee_id),
  work_date date not null,  -- シフトの日付
  zone_id int null references zone(zone_id),          -- 担当区
  job_type_id int null references jobtype(job_type_id), -- 勤務時間帯
  leave_type_id int null references leavetype(leave_type_id),
  special_attendance_id int null references special_attendance_type(special_attendance_id),
  primary key (employee_id, work_date),

  -- 休みor勤務のどちらか一方だけを許す
  check (
    (leave_type_id is not null and job_type_id is null and zone_id is null and special_attendance_id is null)
    or
    (leave_type_id is null and job_type_id is not null and zone_id is not null)
  )
);

create index if not exists idx_shift_date      on shiftassignment(work_date);
create index if not exists idx_shift_zone_date on shiftassignment(zone_id, work_date) where job_type_id is not null;
create index if not exists idx_shift_job_date  on shiftassignment(job_type_id, work_date) where job_type_id is not null;
