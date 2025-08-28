#!/usr/bin/env python3
"""
posms.etl.extractor
===================

Excel テンプレートから DataFrame を生成し、
CSV (data/raw) に保存するユーティリティ。

ファイル対応表
--------------
* excel_templates/input_mail.xlsx  →  data/raw/mail_data_latest.csv
* excel_templates/input_staff.xlsx →  data/raw/staff_data_latest.csv
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Dict

import pandas as pd

LOGGER = logging.getLogger("posms.etl.extractor")


# ---------------------- Helper: logger ----------------------
def _setup_logger() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ---------------------- Main class --------------------------
class ExcelExtractor:
    """
    Parameters
    ----------
    base_dir : Path | None
        プロジェクトルート。None の場合は ``posms`` ディレクトリから2階層上を自動判定。
    mapping : dict[str, str]
        {excel_filename: csv_filename} のマッピング。
    """

    def __init__(
        self,
        base_dir: Path | None = None,
        mapping: Dict[str, str] | None = None,
    ) -> None:
        _setup_logger()

        self.base_dir = base_dir or Path(__file__).resolve().parents[2]
        self.excel_dir = self.base_dir / "excel_templates"
        self.raw_dir = self.base_dir / "data" / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.mapping = mapping or {
            "input_mail.xlsx": "mail_data_latest.csv",
            "input_staff.xlsx": "staff_data_latest.csv",
        }

        LOGGER.info("ExcelExtractor initialized. base_dir=%s", self.base_dir)

    # -------- public API --------
    def extract_file(self, excel_name: str) -> Path:
        """
        指定 Excel を読み込んで CSV に変換 → 保存。
        Returns
        -------
        Path
            保存先 CSV のパス
        """
        src = self.excel_dir / excel_name
        if not src.exists():
            LOGGER.error("Excel ファイルが見つかりません: %s", src)
            raise FileNotFoundError(src)

        dst = self.raw_dir / self.mapping[excel_name]

        df = pd.read_excel(src, sheet_name=0)
        df.to_csv(dst, index=False, encoding="utf-8-sig")
        LOGGER.info("Extracted %s → %s", src.name, dst.name)
        return dst

    def run_all(self) -> None:
        """mapping に登録されたすべての Excel を変換"""
        for excel_name in self.mapping:
            try:
                self.extract_file(excel_name)
            except FileNotFoundError:
                continue

    # -------- convenience for FeatureBuilder etc. --------
    def load_mail_dataframe(self) -> pd.DataFrame:
        """input_mail.xlsx を DataFrame で返す（保存はしない）"""
        return pd.read_excel(self.excel_dir / "input_mail.xlsx", sheet_name=0)

    def load_staff_dataframe(self) -> pd.DataFrame:
        """input_staff.xlsx を DataFrame で返す（保存はしない）"""
        return pd.read_excel(self.excel_dir / "input_staff.xlsx", sheet_name=0)


# ---------------------- CLI entry ---------------------------
def main() -> None:
    ExcelExtractor().run_all()


if __name__ == "__main__":
    main()
