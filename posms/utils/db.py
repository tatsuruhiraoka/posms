# posms/utils/db.py
"""
posms.utils.db
==============

SQLAlchemy の **Engine／Session** を一元管理するヘルパー。

概要
----
- 依存は **環境変数のみ**（`.env` には依存しません）。ゼロ設定の思想に合わせています。
- 接続情報は **`DATABASE_URL` を最優先**し、無ければ `POSTGRES_*`（旧 `DB_*`）から自動組み立て。
- さらに、ローカルの PostgreSQL（localhost:5432）が起動していれば自動検出して使用し、
  それも無い場合は **SQLite**（`excel_templates/posms_demo.db`）へフォールバック。
- 生成した **Engine はプロセス内で再利用**（擬似シングルトン）され、無駄な接続を抑えます。
- `SessionManager().session_scope()` の **with コンテキスト**で自動 commit / rollback / close。

環境変数
--------
- 例: `DATABASE_URL="postgresql+psycopg2://user:pass@host:5432/dbname"`
- または:
  - `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB`
  - 旧名: `DB_USER` / `DB_PASSWORD` / `DB_NAME`

使い方
------
>>> from posms.utils.db import SessionManager
>>> sm = SessionManager()
>>> with sm.session_scope() as s:
...     rows = s.execute(text("SELECT 1")).fetchall()

注意
----
- ライブラリ側では **`logging.basicConfig` や他ロガーのレベル変更は行いません**。
  ログ設定は利用側（アプリ側）で行ってください。
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import Session, sessionmaker

LOGGER = logging.getLogger(__name__)

__all__ = ["SessionManager"]


class SessionManager:
    """Lazy-loaded SQLAlchemy Engine & Session factory."""

    _engine: Engine | None = None
    _session_factory: sessionmaker | None = None

    def __init__(self) -> None:
        # 環境変数ベース（.env 非依存）
        pass

    # ------------------------------------------------------------
    # Engine & Session factory
    # ------------------------------------------------------------
    @property
    def engine(self) -> Engine:
        """共有の SQLAlchemy Engine を返す（未生成なら作成）。"""
        if self._engine is None:
            db_url = self._build_connection_url()
            LOGGER.info("Creating SQLAlchemy engine → %s", db_url)
            self._engine = create_engine(db_url, pool_pre_ping=True, future=True)
        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        """共有の sessionmaker を返す（未生成なら作成）。"""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine, class_=Session, expire_on_commit=False
            )
        return self._session_factory

    # ------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------
    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        """
        with コンテキストで **自動 commit / rollback / close** を行うユーティリティ。

        例:
            >>> sm = SessionManager()
            >>> with sm.session_scope() as s:
            ...     s.execute(text("INSERT ..."))
        """
        session: Session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:  # noqa: BLE001
            session.rollback()
            LOGGER.exception("Session rollback due to exception")
            raise
        finally:
            session.close()

    # ------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------
    @staticmethod
    def _build_connection_url() -> str | URL:
        """
        DB 接続 URL を自動決定（誤検出を避ける安全設計）。

        優先順位:
          1) 明示 `DATABASE_URL`
          2) 明示 `POSTGRES_*` / 旧 `DB_*` で組み立て（HOST が **明示されている時だけ**）
          3) Docker コンテナ **内**なら `db:5432` を使う
          4) それ以外は SQLite（excel_templates/posms_demo.db）
        """

        # 1) 明示 URL 最優先
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            return database_url

        # 2) POSTGRES_* が **揃っていて HOST も明示**されている時だけ採用
        user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
        pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
        host = os.getenv(
            "POSTGRES_HOST"
        )  # 既定は付けない（誤って localhost にならないように）
        port = os.getenv("POSTGRES_PORT") or "5432"
        db = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")
        if all([user, pwd, db, host]):
            return URL.create(
                "postgresql+psycopg2",
                username=user,
                password=pwd,
                host=host,
                port=int(port),
                database=db,
            )

        # 3) Docker コンテナ内なら、compose のサービス名で接続
        #    （/ .dockerenv が存在するのはコンテナ内のみ）
        try:
            if Path("/.dockerenv").exists():
                return "postgresql+psycopg2://posms:posms@db:5432/posms"
        except Exception:
            pass

        # 4) フォールバックは SQLite（配布/Excelモード）
        demo_path = (
            Path(__file__).resolve().parents[2] / "excel_templates" / "posms_demo.db"
        )
        return f"sqlite:///{demo_path}"


# ---------------- Quick self-test ----------------
if __name__ == "__main__":
    # 簡易動作確認（例外はそのまま表示）
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    sm = SessionManager()
    with sm.session_scope() as s:
        version: str = s.execute(text("SELECT version()")).scalar_one()
        print("DB OK:", version)
