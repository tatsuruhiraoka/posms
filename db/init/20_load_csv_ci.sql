\set ON_ERROR_STOP on
SET client_encoding TO 'UTF8';
SET DateStyle = 'ISO, YMD';

TRUNCATE TABLE employee RESTART IDENTITY;
TRUNCATE TABLE job_type RESTART IDENTITY;
TRUNCATE TABLE zone RESTART IDENTITY;
TRUNCATE TABLE demand_profile RESTART IDENTITY;
TRUNCATE TABLE employee_availability RESTART IDENTITY;
TRUNCATE TABLE employee_zone_proficiency RESTART IDENTITY;

\copy employee(employee_code,name,employment_type,position,default_work_hours,paid_leave_remaining,is_certifier,is_active)
  FROM 'db/init/csv/employees.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

\copy job_type(job_type_code,job_type_name,category,for_fulltime_only,default_start_time,default_end_time,is_active)
  FROM 'db/init/csv/jobtypes.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

\copy zone(zone_code,zone_name,is_active)
  FROM 'db/init/csv/zones.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

\copy demand_profile(zone_code,demand_mon,demand_tue,demand_wed,demand_thu,demand_fri,demand_sat,demand_sun,demand_holiday)
  FROM 'db/init/csv/demand_profiles.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

\copy employee_availability(employee_code,work_date,available_from,available_to,shift_type,available)
  FROM 'db/init/csv/employee_availabilities.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

\copy employee_zone_proficiency(employee_code,zone_code,proficiency_level)
  FROM 'db/init/csv/employee_zone_proficiencies.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
