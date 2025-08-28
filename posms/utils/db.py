"""
posms.utils.db
==============

SQLAlchemy Engine／Session を一元管理するヘルパー。

主な機能
--------
* ``SessionManager().session_scope()`` で **with コンテキスト** による
  自動 commit / rollback / close
* ``engine`` プロパティから SQLAlchemy Engine を再利用
* `.env` に設定した **POSTGRES_*** または旧 **DB_*** 変数を自動取得
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

LOGGER = logging.getLogger("posms.utils.db")
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)  # SQL出力は抑制


class SessionManager:
    """Lazy‑loaded SQLAlchemy Engine & Session factory"""

    _engine: Engine | None = None
    _session_factory: sessionmaker | None = None

    def __init__(self) -> None:
        # .env からロード（1回だけ）
        env_path = Path(__file__).resolve().parents[2] / ".env"
        if env_path.exists():
            load_dotenv(env_path)

    # ------------------------------------------------------------
    # Engine & Session factory
    # ------------------------------------------------------------
    @property
    def engine(self) -> Engine:
        if self._engine is None:
            db_url = self._build_connection_url()
            LOGGER.info("Creating SQLAlchemy engine → %s", db_url)
            self._engine = create_engine(db_url, pool_pre_ping=True, future=True)
        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine, class_=Session, expire_on_commit=False)
        return self._session_factory

    # ------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------
    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        """
        ``with SessionManager().session_scope() as sess:`` で
        自動 commit / rollback / close が行われる。
        """
        session = self.session_factory()  # type: Session
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
    def _build_connection_url() -> str:
        """環境変数から Postgres 接続 URL を作成"""
        user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
        pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")

        if not all([user, pwd, db]):
            raise RuntimeError("DB connection info is incomplete (.env を確認)")

        return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


# ---------------- Quick self‑test ----------------
if __name__ == "__main__":
    sm = SessionManager()
    with sm.session_scope() as s:
        version: str = s.execute("SELECT version()").scalar_one()
        print("DB OK:", version)
