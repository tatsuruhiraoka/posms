\set ON_ERROR_STOP on
-- 30_views.sql
-- 各種ビュー定義をまとめる

-- Zone 一覧ビュー（zone_codeを含む参照用）
CREATE OR REPLACE VIEW v_zone_ref AS
SELECT
  o.office_code,
  d.department_code,
  t.team_name,
  z.zone_code,
  z.zone_name,
  z.operational_status,
  z.is_active,
  z.created_at,
  z.updated_at
FROM Zone z
JOIN Team       t ON t.team_id = z.team_id
JOIN Department d ON d.department_id = t.department_id
JOIN Office     o ON o.office_id = d.office_id
ORDER BY d.department_code, t.team_name, z.zone_name;


