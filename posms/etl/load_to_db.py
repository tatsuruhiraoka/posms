#!/usr/bin/env python3
"""
posms.etl.load_to_db
====================

CSV ファイル（data/raw）を PostgreSQL にロードするユーティリティ。
CLI からは ``python -m posms.etl.load_to_db`` または
``poetry run posms load-csv``（Typer 経由）で実行可能。

* staff_data_latest.csv  …  社員マスタを UPSERT
* mail_data_latest.csv   …  郵便物数データを UPSERT
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

LOGGER = logging.getLogger("posms.etl.load_to_db")


# ---------------------- Helper: logger ----------------------
def _setup_logger() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ---------------------- Main class --------------------------
class DbLoader:
    """
    Attributes
    ----------
    conn_url : str
        SQLAlchemy 形式の Postgres 接続 URL
    raw_dir : Path
        CSV ファイルを置くディレクトリ (default: data/raw)
    """

    def __init__(self, base_dir: Path | None = None) -> None:
        _setup_logger()

        base_dir = base_dir or Path(__file__).resolve().parents[2]
        self.raw_dir = base_dir / "data" / "raw"

        # 環境変数を .env からロード
        env_path = base_dir / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        db_user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
        db_pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")

        if not all([db_user, db_pwd, db_name]):
            LOGGER.error("DB connection info is incomplete. Check .env settings.")
            sys.exit(1)

        self.conn_url = f"postgresql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
        self.engine = create_engine(self.conn_url)
        LOGGER.info("DbLoader initialized: %s", self.conn_url)

    # -------- public API --------
    def upsert_staff(self, csv_path: Path | None = None) -> int:
        """CSV から employees テーブルへ UPSERT"""
        csv_path = csv_path or self.raw_dir / "staff_data_latest.csv"
        if not csv_path.exists():
            LOGGER.warning("Staff CSV not found: %s", csv_path)
            return 0

        df_staff = pd.read_csv(csv_path)
        names = df_staff["name"].dropna().astype(str).unique()
        inserted = 0
        with self.engine.begin() as conn:
            for name in names:
                res = conn.execute(
                    text("SELECT id FROM employees WHERE name = :name"), {"name": name}
                ).fetchone()
                if res is None:
                    conn.execute(
                        text("INSERT INTO employees (name) VALUES (:name)"),
                        {"name": name},
                    )
                    inserted += 1
                    LOGGER.info("Inserted new employee: %s", name)
        return inserted

    def upsert_mail(self, csv_path: Path | None = None) -> int:
        """CSV から mail_data テーブルへ UPSERT (同日を置換)"""
        csv_path = csv_path or self.raw_dir / "mail_data_latest.csv"
        if not csv_path.exists():
            LOGGER.warning("Mail CSV not found: %s", csv_path)
            return 0

        df_mail = pd.read_csv(csv_path, parse_dates=["mail_date"])
        dates: Iterable[str] = df_mail["mail_date"].dt.date.tolist()
        with self.engine.begin() as conn:
            conn.execute(
                text("DELETE FROM mail_data WHERE mail_date = ANY(:dates)"),
                {"dates": dates},
            )
            df_mail.to_sql("mail_data", conn, if_exists="append", index=False)
        LOGGER.info("Loaded %d records into mail_data", len(df_mail))
        return len(df_mail)

    def run_all(self) -> None:
        """スタッフ → 郵便物データの順にロード"""
        self.upsert_staff()
        self.upsert_mail()


# ---------------------- CLI entry ---------------------------
def main() -> None:
    loader = DbLoader()
    loader.run_all()


if __name__ == "__main__":
    main()
