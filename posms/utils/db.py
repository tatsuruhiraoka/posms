# posms/utils/db.py
"""
posms.utils.db
==============

SQLAlchemy の **Engine／Session** を一元管理するヘルパー。

概要
----
- 依存は **環境変数のみ**（`.env` には依存しません）。ゼロ設定の思想に合わせています。
- 接続情報は **`DATABASE_URL` を最優先**し、無ければ `POSTGRES_*`（旧 `DB_*`）から自動組み立て。
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
        # 現状は環境変数のみを前提とする（.env 非依存）
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
        環境変数から Postgres 接続 URL を作成。

        優先順位:
        1) `DATABASE_URL`（例: postgresql+psycopg2://user:pass@host:5432/db）
        2) `POSTGRES_*` / 旧 `DB_*` を `URL.create` で組み立て
        """
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            return database_url

        user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
        pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")

        if not all([user, pwd, db]):
            raise RuntimeError(
                "DB connection info is incomplete (set env vars: DATABASE_URL or POSTGRES_*)"
            )

        # URL.create で安全に組み立て（パスワード中の記号にも強い）
        return URL.create(
            "postgresql+psycopg2",
            username=user,
            password=pwd,
            host=host,
            port=int(port) if port else None,
            database=db,
        )


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
