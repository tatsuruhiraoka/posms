# posms/etl/load_to_db.py
#!/usr/bin/env python3
"""
posms.etl.load_to_db
====================

CSV（<repo>/data/raw）を PostgreSQL にロードするユーティリティ。

- staff_data_latest.csv … Employee テーブルへ UPSERT（衝突キー: employee_code）

設計方針
--------
- .env は使わない（ゼロ設定）。接続情報は環境変数から読む：
  - 優先: DATABASE_URL（例: postgresql+psycopg2://user:pass@host:5432/db）
  - 代替: POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DB
- ライブラリ層で sys.exit は行わず、例外を送出
- ログ設定（basicConfig）は呼び出し側に委ねる
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import URL, Engine

LOGGER = logging.getLogger(__name__)


class DbLoader:
    """
    CSV を DB にロードするヘルパー。

    Parameters
    ----------
    base_dir : Path | None
        プロジェクトルート。None の場合はこのファイルから 2 階層上（<repo>）を推定。
    engine : sqlalchemy.engine.Engine | None
        既存の Engine を渡す場合。None なら環境変数から自動作成。
    """

    def __init__(
        self, base_dir: Path | None = None, engine: Engine | None = None
    ) -> None:
        self.base_dir = base_dir or Path(__file__).resolve().parents[2]
        self.raw_dir = self.base_dir / "data" / "raw"

        if engine is not None:
            self.engine = engine
        else:
            self.engine = self._make_engine_from_env()

        LOGGER.info("DbLoader initialized: %s", self.engine.url)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def upsert_staff(self, csv_path: Path | None = None) -> int:
        """
        staff_data_latest.csv を employee テーブルへ UPSERT（衝突キー: employee_code）

        必須列:
            employee_code, name, employment_type,
            default_work_hours, monthly_work_hours, team_id
        任意列:
            position, is_certifier

        Returns
        -------
        int
            影響件数（INSERT + UPDATE）
        """
        csv_path = csv_path or self.raw_dir / "staff_data_latest.csv"
        if not csv_path.exists():
            LOGGER.warning("Staff CSV not found: %s", csv_path)
            return 0

        df = pd.read_csv(csv_path)

        required = [
            "employee_code",
            "name",
            "employment_type",
            "default_work_hours",
            "monthly_work_hours",
            "team_id",
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"staff CSV に必須列が不足しています: {missing}")

        # 型・値の正規化
        df["employee_code"] = df["employee_code"].astype(str).strip()
        df["name"] = df["name"].astype(str).str.strip()
        df["employment_type"] = df["employment_type"].astype(str).str.strip()

        for c in ("default_work_hours", "monthly_work_hours", "team_id"):
            if df[c].isna().any():
                raise ValueError(f"列 {c} に欠損があります（NOT NULL 制約に違反）")
            df[c] = pd.to_numeric(df[c], errors="raise").astype(int)

        # 制約レンジの事前バリデーション（DBで落ちる前に検知）
        if not df["default_work_hours"].between(1, 24).all():
            bad = df.loc[
                ~df["default_work_hours"].between(1, 24), "default_work_hours"
            ].unique()
            raise ValueError(f"default_work_hours の値域外: {bad.tolist()}")
        if not df["monthly_work_hours"].between(1, 300).all():
            bad = df.loc[
                ~df["monthly_work_hours"].between(1, 300), "monthly_work_hours"
            ].unique()
            raise ValueError(f"monthly_work_hours の値域外: {bad.tolist()}")

        # 任意列の初期化
        if "is_certifier" in df.columns:
            truthy = {"1", "true", "t", "y", "yes", "on"}
            df["is_certifier"] = (
                df["is_certifier"].astype(str).str.lower().isin(truthy).astype(bool)
            )
        else:
            df["is_certifier"] = False
        if "position" not in df.columns:
            df["position"] = None

        rows = df[
            [
                "employee_code",
                "name",
                "employment_type",
                "position",
                "default_work_hours",
                "monthly_work_hours",
                "team_id",
                "is_certifier",
            ]
        ].to_dict(orient="records")
        if not rows:
            LOGGER.info("No staff rows to upsert.")
            return 0

        meta = MetaData()
        # 注意: 未引用 CREATE TABLE Employee は PostgreSQL 内部名 'employee' になります
        employee_tbl = Table("employee", meta, autoload_with=self.engine)

        stmt = pg_insert(employee_tbl).values(rows)

        update_cols = {
            "name": stmt.excluded.name,
            "employment_type": stmt.excluded.employment_type,
            "position": stmt.excluded.position,
            "default_work_hours": stmt.excluded.default_work_hours,
            "monthly_work_hours": stmt.excluded.monthly_work_hours,
            "team_id": stmt.excluded.team_id,
            "is_certifier": stmt.excluded.is_certifier,
            "updated_at": func.now(),
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=[employee_tbl.c.employee_code],
            set_=update_cols,
        )

        with self.engine.begin() as conn:
            result = conn.execute(stmt)

        affected = int(result.rowcount or 0)
        LOGGER.info("Upsert staff done: affected=%d", affected)
        return affected

    # -------------- 汎用 CSV → UPSERT（任意で利用可） ---------------- #
    def upsert_csv(
        self,
        csv_path: Path,
        table_name: str,
        conflict_cols: List[str],
        *,
        dtype: Optional[Dict[str, str]] = None,
        transforms: Optional[List] = None,
        set_updated_at: bool = True,
    ) -> int:
        """
        任意の CSV を任意のテーブルへ UPSERT する汎用ヘルパー。

        Parameters
        ----------
        csv_path : Path
            入力 CSV
        table_name : str
            反映先テーブル名（スキーマ既定は public）
        conflict_cols : list[str]
            ON CONFLICT のキー列（インデックス or 一意制約が必要）
        dtype : dict[str, str] | None
            pandas.read_csv の dtype 指定
        transforms : list[callable] | None
            DataFrame を受け取り、加工して返す関数のリスト
        set_updated_at : bool
            テーブルに updated_at がある場合、サーバ時刻で更新する

        Returns
        -------
        int
            影響件数（INSERT + UPDATE）
        """
        if not csv_path.exists():
            LOGGER.warning("CSV not found: %s", csv_path)
            return 0

        df = pd.read_csv(csv_path, dtype=dtype)
        if transforms:
            for f in transforms:
                df = f(df)

        meta = MetaData()
        tbl = Table(table_name, meta, autoload_with=self.engine)
        table_cols = {c.name for c in tbl.columns}

        # CSV 側の列とテーブル列の共通部分だけを対象にする
        cols = [c for c in df.columns if c in table_cols]
        if not cols:
            raise ValueError("CSV の列とテーブル列に共通部分がありません")
        rows = df[cols].to_dict(orient="records")
        if not rows:
            return 0

        stmt = pg_insert(tbl).values(rows)
        update_map = {
            c: getattr(stmt.excluded, c) for c in cols if c not in conflict_cols
        }
        if set_updated_at and "updated_at" in table_cols:
            update_map["updated_at"] = func.now()

        stmt = stmt.on_conflict_do_update(
            index_elements=[tbl.c[c] for c in conflict_cols], set_=update_map
        )

        with self.engine.begin() as conn:
            result = conn.execute(stmt)

        return int(result.rowcount or 0)

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #
    def _make_engine_from_env(self) -> Engine:
        """環境変数から SQLAlchemy Engine を作成。"""
        database_url = os.getenv("DATABASE_URL")

        if database_url:
            url = database_url  # そのまま使う（postgresql+psycopg2://...）
        else:
            user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
            pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            name = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")

            if not all([user, pwd, name]):
                raise RuntimeError(
                    "DB connection info is incomplete. "
                    "Set DATABASE_URL or POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB."
                )

            url = URL.create(
                "postgresql+psycopg2",
                username=user,
                password=pwd,
                host=host,
                port=int(port) if port else None,
                database=name,
            )

        return create_engine(url, pool_pre_ping=True, future=True)


__all__ = ["DbLoader"]


if __name__ == "__main__":
    # ライブラリ外で直接実行された場合のみ、簡易スクリプトとして動作
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    loader = DbLoader()
    try:
        affected = loader.upsert_staff()
        print(f"Upsert staff affected={affected}")
    except Exception as e:
        LOGGER.error("Load failed: %s", e)
        raise
