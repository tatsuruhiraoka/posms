\set ON_ERROR_STOP on
-- 10_seed_masters.sql

begin;

-- ========= office =========
insert into office (office_name, office_code, is_active, updated_at)
values ('局', 'HQ', 1, now())
on conflict (office_code) do update
set office_name = excluded.office_name,
    is_active   = excluded.is_active,
    updated_at  = now();

-- ========= department =========
insert into department (office_id, department_name, department_code, is_active, updated_at)
values
  ((select office_id from office where office_code = 'HQ'), '第一集配営業部', 'DPT-A', 1, now()),
  ((select office_id from office where office_code = 'HQ'), '第二集配営業部', 'DPT-B', 1, now())
on conflict (department_code) do update
set department_name = excluded.department_name,
    office_id       = excluded.office_id,
    is_active       = excluded.is_active,
    updated_at      = now();

-- ========= team =========
with d1 as (
  select department_id from department where department_code = 'DPT-A'
),
d2 as (
  select department_id from department where department_code = 'DPT-B'
)
insert into team (department_id, team_name, is_active, updated_at)
select d1.department_id, t, 1, now() from d1, unnest(array['1班','2班','3班','4班','5班']) as t
union all
select d2.department_id, t, 1, now() from d2, unnest(array['6班','7班','8班','9班']) as t
on conflict (department_id, team_name) do update
set is_active  = excluded.is_active,
    updated_at = now();

-- ========= leavetype =========
insert into leavetype (leave_code, leave_name, leave_category, updated_at) values
  ('OFF_DUTY',            '非番',   'REGULAR', now()),
  ('WEEKLY_OFF',          '週休',   'REGULAR', now()),
  ('PUBLIC_HOLIDAY_OFF',  '祝休',   'REGULAR', now()),
  ('PLANNED_ANNUAL_LEAVE','計年',   'SPECIAL', now()),
  ('ANNUAL_LEAVE',        '年休',   'SPECIAL', now()),
  ('SUMMER_LEAVE',        '夏期',   'SPECIAL', now()),
  ('WINTER_LEAVE',        '冬期',   'SPECIAL', now()),
  ('COMPENSATORY_LEAVE',  '代休',   'SPECIAL', now()),
  ('APPROVED_ABSENCE',    '承欠',   'SPECIAL', now()),
  ('MATERNITY_LEAVE',     '産休',   'SPECIAL', now()),
  ('PARENTAL_LEAVE',      '育休',   'SPECIAL', now()),
  ('CAREGIVER_LEAVE',     '介護',   'SPECIAL', now()),
  ('SICK_LEAVE',          '病休',   'SPECIAL', now()),
  ('LEAVE_OF_ABSENCE',    '休職',   'SPECIAL', now()),
  ('OTHER',               'その他', 'SPECIAL', now())
on conflict (leave_code) do update
set leave_name     = excluded.leave_name,
    leave_category = excluded.leave_category,
    updated_at     = now();

-- ========= special_attendance_type（廃休・マル超） =========
insert into special_attendance_type
  (attendance_code, attendance_name, holiday_work_flag, is_active, updated_at)
values
  ('HAIKYU',  '廃休',  0, 1, now()),
  ('MARUCHO', 'マル超', 0, 1, now())
on conflict (attendance_code) do update
set attendance_name   = excluded.attendance_name,
    holiday_work_flag = excluded.holiday_work_flag,
    is_active         = excluded.is_active,
    updated_at        = now();

commit;

-- zone_code 自動採番
create sequence if not exists zone_code_seq;

create or replace function gen_zone_code()
returns trigger as $$
begin
  if new.zone_code is null or btrim(new.zone_code) = '' then
    new.zone_code := 'Z' || to_char(nextval('zone_code_seq'), 'FM000000');
  end if;
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_zone_code on zone;
create trigger trg_zone_code
before insert on zone
for each row execute function gen_zone_code();
