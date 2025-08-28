# tests/etl/conftest.py
from pathlib import Path
import pandas as pd
import pytest


@pytest.fixture
def tmp_excel_project(tmp_path: Path) -> Path:
    """data/raw, excel_templates をもつ一時プロジェクト構造を作成し Path を返す"""
    # ディレクトリ
    (tmp_path / "excel_templates").mkdir(parents=True)
    (tmp_path / "data" / "raw").mkdir(parents=True)

    # ダミー Excel 1
    df_mail = pd.DataFrame(
        {
            "mail_date": ["2025-08-01", "2025-08-02"],
            "mail_count": [10000, 12000],
            "is_holiday": [False, False],
            "price_increase_flag": [False, False],
        }
    )
    df_mail.to_excel(tmp_path / "excel_templates" / "input_mail.xlsx", index=False)

    # ダミー Excel 2
    df_staff = pd.DataFrame({"name": ["社員A", "社員B"]})
    df_staff.to_excel(tmp_path / "excel_templates" / "input_staff.xlsx", index=False)

    return tmp_path
