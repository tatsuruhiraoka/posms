\set ON_ERROR_STOP on
\timing on

-- デフォルトのCSVルート（コンテナ内）。CI等で上書き可: -v csvdir='db/init/csv'
\if :{?csvdir} \else \set csvdir '/docker-entrypoint-initdb.d/csv' \endif

-- ★ zoneの既定ステータス（未指定時）
\if :{?default_zone_status} \else \set default_zone_status '通配' \endif
-- 祝日CSV名（未指定なら同梱のファイル名）
\if :{?holiday_csv} \else \set holiday_csv 'holidays_jp_2020_2050.csv' \endif

-- 各CSVのフルパスをpsql変数に格納（COPYは単一文字列しか受けないため）
select
  (:'csvdir' || '/jobtypes.csv')                      as jobtypes_file,
  (:'csvdir' || '/zones.csv')                         as zones_file,
  (:'csvdir' || '/demand_profiles.csv')               as demand_profiles_file,
  (:'csvdir' || '/postal_datas.csv')                  as postal_datas_file,
  (:'csvdir' || '/employees.csv')                     as employees_file,
  (:'csvdir' || '/employee_zone_proficiencies.csv')   as ezp_file,
  (:'csvdir' || '/employee_availabilities.csv')       as eavail_file,
  (:'csvdir' || '/' || :'holiday_csv')                as holiday_file
\gset

------------------------------------------------------------
-- 1) jobtype  (jobtypes.csv)
--   columns: classification,job_code,job_name,start_time,end_time,work_hours
------------------------------------------------------------
drop table if exists stage_jobtype;
create temp table stage_jobtype (
  classification text,
  job_code       text,
  job_name       text,
  start_time     time,
  end_time       time,
  work_hours     int
);
copy stage_jobtype from :'jobtypes_file' with (format csv, header true, encoding 'UTF8');

begin;
insert into jobtype (job_code, classification, job_name, start_time, end_time, work_hours, updated_at)
select job_code, classification, job_name, start_time, end_time, work_hours, now()
from stage_jobtype
on conflict (job_code) do update
set  classification = excluded.classification
   , job_name       = excluded.job_name
   , start_time     = excluded.start_time
   , end_time       = excluded.end_time
   , work_hours     = excluded.work_hours
   , updated_at     = now();
commit;

------------------------------------------------------------
-- 2) zone  (zones.csv)
--   columns: department_code,team_name,zone_name,operational_status,shift_type
------------------------------------------------------------
drop table if exists stage_zone;
create temp table stage_zone (
  department_code    text,
  team_name          text,
  zone_name          text,
  operational_status text,
  shift_type         text
);
copy stage_zone from :'zones_file' with (format csv, header true, encoding 'UTF8');

begin;
insert into zone (team_id, zone_name, operational_status, is_active, shift_type, updated_at)
select
  t.team_id,
  s.zone_name,
  coalesce(nullif(btrim(s.operational_status), ''), :'default_zone_status'),
  1,
  coalesce(nullif(btrim(s.shift_type), ''), '日勤'),
  now()
from stage_zone s
join department d on d.department_code = s.department_code
join team       t on t.department_id   = d.department_id
                 and t.team_name       = s.team_name
on conflict on constraint zone_unique_per_team
do update set
  operational_status = excluded.operational_status,
  shift_type         = excluded.shift_type,
  is_active          = 1,
  updated_at         = now();
commit;

------------------------------------------------------------
-- 3) demandprofile  (demand_profiles.csv)
--   columns: department_code,team_name,zone_name,demand_mon,...,demand_holiday
------------------------------------------------------------
drop table if exists stage_demand_profile;
create temp table stage_demand_profile (
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
copy stage_demand_profile from :'demand_profiles_file' with (format csv, header true, encoding 'UTF8');

begin;
with team_resolved as (
  select s.*, d.department_id, t.team_id
  from stage_demand_profile s
  join department d on d.department_code = s.department_code
  join team       t on t.department_id   = d.department_id
                   and t.team_name       = s.team_name
),
zone_resolved as (
  select tr.*, z.zone_id
  from team_resolved tr
  join zone z on z.team_id = tr.team_id and z.zone_name = tr.zone_name
)
insert into demandprofile
  (zone_id, demand_mon, demand_tue, demand_wed, demand_thu, demand_fri, demand_sat, demand_sun, demand_holiday)
select zr.zone_id, zr.demand_mon, zr.demand_tue, zr.demand_wed, zr.demand_thu, zr.demand_fri, zr.demand_sat, zr.demand_sun, zr.demand_holiday
from zone_resolved zr
on conflict (zone_id) do update
set  demand_mon     = excluded.demand_mon
   , demand_tue     = excluded.demand_tue
   , demand_wed     = excluded.demand_wed
   , demand_thu     = excluded.demand_thu
   , demand_fri     = excluded.demand_fri
   , demand_sat     = excluded.demand_sat
   , demand_sun     = excluded.demand_sun
   , demand_holiday = excluded.demand_holiday;
commit;

------------------------------------------------------------
-- 4) mailvolume_by_type  (postal_datas.csv)
--   columns: date,normal,registered,lp_plus,nenga_assembly,nenga_delivery
------------------------------------------------------------
\if :{?office_code} \else \set office_code 'HQ' \endif

drop table if exists stage_postal_datas;
create temp table stage_postal_datas (
  date           date,
  normal         int,
  registered     int,
  lp_plus        int,
  nenga_assembly int,
  nenga_delivery int
);

-- ★ 英語ヘッダーをこの順番で読み取る（CSVと一致させる）
copy stage_postal_datas (
  date,
  normal,
  registered,
  lp_plus,
  nenga_assembly,
  nenga_delivery
)
from :'postal_datas_file'
with (format csv, header true, encoding 'UTF8');

begin;

-- 通常
insert into mailvolume_by_type (
  date, office_id, mail_kind,
  actual_volume, forecast_volume, price_increase_flag,
  created_at, updated_at
)
select s.date, o.office_id, 'normal',
       s.normal, null, 0, now(), now()
from stage_postal_datas s
join office o on o.office_code = :'office_code'
where s.normal is not null
on conflict (date, office_id, mail_kind)
do update set
  actual_volume        = excluded.actual_volume,
  forecast_volume      = excluded.forecast_volume,
  price_increase_flag  = excluded.price_increase_flag,
  updated_at           = now();

-- 書留
insert into mailvolume_by_type
select s.date, o.office_id, 'registered',
       s.registered, null, 0, now(), now()
from stage_postal_datas s
join office o on o.office_code = :'office_code'
where s.registered is not null
on conflict (date, office_id, mail_kind)
do update set
  actual_volume        = excluded.actual_volume,
  forecast_volume      = excluded.forecast_volume,
  price_increase_flag  = excluded.price_increase_flag,
  updated_at           = now();

-- レターパックプラス
insert into mailvolume_by_type
select s.date, o.office_id, 'lp_plus',
       s.lp_plus, null, 0, now(), now()
from stage_postal_datas s
join office o on o.office_code = :'office_code'
where s.lp_plus is not null
on conflict (date, office_id, mail_kind)
do update set
  actual_volume        = excluded.actual_volume,
  forecast_volume      = excluded.forecast_volume,
  price_increase_flag  = excluded.price_increase_flag,
  updated_at           = now();

-- 年賀組立
insert into mailvolume_by_type
select s.date, o.office_id, 'nenga_assembly',
       s.nenga_assembly, null, 0, now(), now()
from stage_postal_datas s
join office o on o.office_code = :'office_code'
where s.nenga_assembly is not null
on conflict (date, office_id, mail_kind)
do update set
  actual_volume        = excluded.actual_volume,
  forecast_volume      = excluded.forecast_volume,
  price_increase_flag  = excluded.price_increase_flag,
  updated_at           = now();

-- 年賀配達
insert into mailvolume_by_type
select s.date, o.office_id, 'nenga_delivery',
       s.nenga_delivery, null, 0, now(), now()
from stage_postal_datas s
join office o on o.office_code = :'office_code'
where s.nenga_delivery is not null
on conflict (date, office_id, mail_kind)
do update set
  actual_volume        = excluded.actual_volume,
  forecast_volume      = excluded.forecast_volume,
  price_increase_flag  = excluded.price_increase_flag,
  updated_at           = now();

commit;

------------------------------------------------------------
-- 5) employee  (employees.csv)
--   columns:
--   employee_code,name,employment_type,position,
--   is_leader,is_vice_leader,default_work_hours,monthly_work_hours,
--   department_code,team_name,is_certifier
------------------------------------------------------------
drop table if exists stage_employees;
create temp table stage_employees (
  employee_code      text,
  name               text,
  employment_type    text,
  position           text,
  is_leader          int,
  is_vice_leader     int,
  default_work_hours int,
  monthly_work_hours int,
  department_code    text,
  team_name          text,
  is_certifier       text
);

-- ヘッダ順に列リストを明示（CSVと一致させる）
copy stage_employees (
  employee_code, name, employment_type, position,
  is_leader, is_vice_leader,
  default_work_hours, monthly_work_hours,
  department_code, team_name, is_certifier
)
from :'employees_file' with (format csv, header true, encoding 'UTF8');

begin;
insert into employee
  (employee_code, name, employment_type, position,
   default_work_hours, monthly_work_hours, team_id,
   is_leader, is_vice_leader, is_certifier,
   created_at, updated_at)
select
  btrim(s.employee_code)                    as employee_code,
  coalesce(s.name,'')                       as name,
  coalesce(s.employment_type,'')            as employment_type,
  nullif(btrim(coalesce(s.position,'')),'') as position,
  s.default_work_hours,
  s.monthly_work_hours,
  t.team_id,
  coalesce(s.is_leader,0),
  coalesce(s.is_vice_leader,0),
  case when upper(btrim(coalesce(s.is_certifier,''))) in ('TRUE','T','1','Y') then 1 else 0 end,
  now(), now()
from stage_employees s
join department d on d.department_code = s.department_code
join team       t on t.department_id   = d.department_id
                 and t.team_name       = s.team_name
on conflict (employee_code) do update
set  name               = excluded.name
   , employment_type    = excluded.employment_type
   , position           = excluded.position
   , default_work_hours = excluded.default_work_hours
   , monthly_work_hours = excluded.monthly_work_hours
   , team_id            = excluded.team_id
   , is_leader          = excluded.is_leader
   , is_vice_leader     = excluded.is_vice_leader
   , is_certifier       = excluded.is_certifier
   , updated_at         = now();
commit;

------------------------------------------------------------
-- 6) employeezoneproficiency  (employee_zone_proficiencies.csv)
--   columns: employee_code,zone_name,team_name,department_code,proficiency
------------------------------------------------------------
drop table if exists stage_ezp;
create temp table stage_ezp (
  employee_code text,
  name          text,
  department_code text,
  team_name     text,
  zone_name     text,
  proficiency   int
);
copy stage_ezp from :'ezp_file' with (format csv, header true, encoding 'UTF8');

begin;
with team_resolved as (
  select s.*, d.department_id, t.team_id, e.employee_id
  from stage_ezp s
  join department d on d.department_code = s.department_code
  join team       t on t.department_id   = d.department_id and t.team_name = s.team_name
  join employee   e on e.employee_code   = s.employee_code
),
zone_resolved as (
  select tr.*, z.zone_id
  from team_resolved tr
  join zone z on z.team_id = tr.team_id and z.zone_name = tr.zone_name
)
insert into employeezoneproficiency (employee_id, zone_id, proficiency, updated_at)
select zr.employee_id, zr.zone_id, coalesce(zr.proficiency,0), now()
from zone_resolved zr
on conflict (employee_id, zone_id) do update
set  proficiency = excluded.proficiency
   , updated_at  = now();
commit;

------------------------------------------------------------
-- 7) employeeavailability  (employee_availabilities.csv)
--   columns: employee_code,available_mon,...,available_hol  (0/1)
------------------------------------------------------------
drop table if exists stage_eavail;
create temp table stage_eavail (
  employee_code text,
  available_mon int,
  available_tue int,
  available_wed int,
  available_thu int,
  available_fri int,
  available_sat int,
  available_sun int,
  available_hol int,
  available_early     text,
  available_day       text,
  available_mid       text,
  available_night     text,
  available_night_sat text,
  available_night_sun text,
  available_night_hol text
);
copy stage_eavail from :'eavail_file' with (format csv, header true, encoding 'UTF8');

begin;
insert into employeeavailability
  (employee_id, available_mon, available_tue, available_wed, available_thu, available_fri, available_sat, available_sun, available_hol)
select e.employee_id,
       coalesce(s.available_mon,1),
       coalesce(s.available_tue,1),
       coalesce(s.available_wed,1),
       coalesce(s.available_thu,1),
       coalesce(s.available_fri,1),
       coalesce(s.available_sat,0),
       coalesce(s.available_sun,0),
       coalesce(s.available_hol,0)
from stage_eavail s
join employee e on e.employee_code = s.employee_code
on conflict (employee_id) do update
set  available_mon = excluded.available_mon
   , available_tue = excluded.available_tue
   , available_wed = excluded.available_wed
   , available_thu = excluded.available_thu
   , available_fri = excluded.available_fri
   , available_sat = excluded.available_sat
   , available_sun = excluded.available_sun
   , available_hol = excluded.available_hol;
commit;

------------------------------------------------------------
-- 8) holiday  (holidays_jp_2020_2050.csv)
------------------------------------------------------------
drop table if exists stage_holiday;
create temp table stage_holiday (
  holiday_date date,
  name         text
);
copy stage_holiday from :'holiday_file' with (format csv, header true, encoding 'UTF8');

begin;
insert into holiday (holiday_date, name)
select holiday_date, name
from stage_holiday
on conflict (holiday_date) do update
set name = excluded.name;
commit;
