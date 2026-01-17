\set ON_ERROR_STOP on
-- 30_views.sql
-- 各種ビュー定義をまとめる

-- zone 一覧ビュー（zone_codeを含む参照用）
create or replace view v_zone_ref as
select
  o.office_code,
  d.department_code,
  t.team_name,
  z.zone_code,
  z.zone_name,
  z.operational_status,
  z.is_active,
  z.created_at,
  z.updated_at
from zone z
join team       t on t.team_id = z.team_id
join department d on d.department_id = t.department_id
join office     o on o.office_id = d.office_id
order by d.department_code, t.team_name, z.zone_name;
