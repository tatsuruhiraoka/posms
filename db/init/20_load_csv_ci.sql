-- db/init/20_load_csv_ci.sql  (CI 専用ドライバ)
\set ON_ERROR_STOP on
\timing on
set client_encoding to 'UTF8';
set datestyle to 'ISO, YMD';

-- CSV ルート（リポ内を参照）
\set csvdir 'db/init/csv'

-- zone の既定ステータス（CSVが空のときに使う。必要に応じて変更可）
\set default_zone_status '通配'

-- mailvolume を入れる office_code（必要なコードに合わせて）
\set office_code 'HQ'

-- holiday CSV（事前生成したもの）
\set holiday_csv 'holidays_jp_2020_2050.csv'

-- （オプション）完全ワイプしたい場合だけ -v wipe=1 を付けて実行
\if :{?wipe}
truncate table
  employeeavailability,
  employeezoneproficiency,
  demandprofile,
  zone,
  employee,
  jobtype,
  mailvolume_by_type,
  holiday
restart identity cascade;
\endif

-- 本番と同じローダーを呼び出す（ステージング→UPSERT なので外部キーも安全）
\i db/init/20_load_csv.sql

-- 統計更新（読み性能の安定化）
analyze jobtype, zone, demandprofile, mailvolume_by_type,
        employee, employeezoneproficiency, employeeavailability, holiday;
