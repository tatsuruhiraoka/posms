# tests/etl/test_extractor.py
from pathlib import Path
import pandas as pd
from posms.etl.extractor import ExcelExtractor


def test_extract_excel_to_csv(tmp_excel_project: Path):
    """ExcelExtractor が CSV を正しく生成するか"""
    extractor = ExcelExtractor(base_dir=tmp_excel_project)
    extractor.run_all()

    raw_dir = tmp_excel_project / "data" / "raw"
    mail_csv = raw_dir / "mail_data_latest.csv"
    staff_csv = raw_dir / "staff_data_latest.csv"

    # ファイルが生成されているか
    assert mail_csv.exists(), "mail CSV が生成されていない"
    assert staff_csv.exists(), "staff CSV が生成されていない"

    # 内容を軽く検証
    df_mail = pd.read_csv(mail_csv)
    assert len(df_mail) == 2
    assert df_mail["mail_count"].iloc[0] == 10000
