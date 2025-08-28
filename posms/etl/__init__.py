"""
posms.etl
=========

ETL（Extract‑Transform‑Load） サブパッケージ。

* ExcelExtractor …… Excel テンプレートから pandas.DataFrame へ
* DbLoader        …… DataFrame を PostgreSQL に UPSERT

Example
-------
>>> from posms.etl import ExcelExtractor, DbLoader
>>> df_mail, df_staff = ExcelExtractor().load_all("excel_templates")
>>> DbLoader().upsert_mail(df_mail)
"""

from __future__ import annotations

# サブモジュールの公開 API をトップレベルで re‑export
try:
    from .extractor import ExcelExtractor  # noqa: F401
    from .loader import DbLoader          # noqa: F401
except ModuleNotFoundError:
    # 開発初期でまだ実装ファイルが無い場合でもパッケージ import が失敗しないように
    pass

__all__: list[str] = ["ExcelExtractor", "DbLoader"]
