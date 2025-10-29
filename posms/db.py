# posms/db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def _make_engine_from_env() -> Engine:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("環境変数 DATABASE_URL を設定してください。")
    return create_engine(url, future=True)
