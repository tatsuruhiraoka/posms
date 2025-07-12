-- ------- Schema Reset (development only) -------
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- ------- Employees -----------------------------
CREATE TABLE employees (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

-- ------- Mail Volume ---------------------------
CREATE TABLE mail_data (
    mail_date            DATE    PRIMARY KEY,
    mail_count           INTEGER NOT NULL,
    is_holiday           BOOLEAN NOT NULL DEFAULT FALSE,
    price_increase_flag  BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_mail_date ON mail_data(mail_date);

-- ------- Shift Results -------------------------
CREATE TABLE shifts (
    id          SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    shift_date  DATE    NOT NULL,
    shift_type  VARCHAR(50) NOT NULL,
    job         VARCHAR(100) NOT NULL,
    UNIQUE (employee_id, shift_date)
);

CREATE INDEX idx_shift_date ON shifts(shift_date);

-- ------- Seed Data -----------------------------
INSERT INTO employees (name) VALUES
  ('社員A'),
  ('社員B'),
  ('社員C');

-- サンプル郵便物データ（１週間）
INSERT INTO mail_data (mail_date, mail_count, is_holiday, price_increase_flag) VALUES
  ('2025-07-01', 12000, FALSE, FALSE),
  ('2025-07-02', 11500, FALSE, FALSE),
  ('2025-07-03', 11800, FALSE, FALSE),
  ('2025-07-04', 11300, FALSE, FALSE),
  ('2025-07-05',  9500, TRUE , FALSE),
  ('2025-07-06',  9800, TRUE , FALSE),
  ('2025-07-07', 12100, FALSE, FALSE);

-- Note: shifts テーブルはアプリ側で INSERT / UPSERT します
