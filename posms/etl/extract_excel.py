# posms/etl/extract_excel.py
#!/usr/bin/env python3
"""
posms.etl.extract_excel
=======================

Excel テンプレートから pandas.DataFrame を生成し、
CSV（<repo>/data/raw）に保存するユーティリティ。

ファイル対応表（既定）
----------------------
* excel_templates/input_mail.xlsx  →  data/raw/mail_data_latest.csv
* excel_templates/input_staff.xlsx →  data/raw/staff_data_latest.csv

方針
----
- ライブラリ側では logging.basicConfig を呼ばない（呼び出し元に委ねる）
- 文字化け対策のため CSV の既定エンコーディングは 'utf-8-sig'
- Excel 読み込みは 'openpyxl' を既定エンジンとして明示
- .env には依存しない（ゼロ設定）
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)


class ExcelExtractor:
    """
    Excel テンプレート群を CSV に変換して保存します。

    Parameters
    ----------
    base_dir : Path | None
        プロジェクトルート。None の場合はこのファイルから **2階層上** を自動判定。
        （<repo>/posms/etl/extract_excel.py → <repo>）
    mapping : dict[str, str] | None
        {excel_filename: csv_filename} のマッピング。
        既定は {"input_mail.xlsx": "mail_data_latest.csv",
               "input_staff.xlsx": "staff_data_latest.csv"}。
    csv_encoding : str
        CSV のテキストエンコーディング。既定は 'utf-8-sig'（Excelとの相性重視）。
    excel_engine : str
        pandas.read_excel のエンジン（既定 'openpyxl'）。
    sheet : str | int | None
        読み込むシート名/番号。既定 0（先頭シート）。
    transform : Callable[[pd.DataFrame, str], pd.DataFrame] | None
        読み込み後に DataFrame を加工する関数。引数は (df, excel_name)。
        例：列名正規化、型変換など。None の場合は無加工。
    atomic : bool
        CSV 保存をアトミックに行う（テンポラリ→リネーム）。既定 True。
    """

    def __init__(
        self,
        base_dir: Path | None = None,
        mapping: Dict[str, str] | None = None,
        *,
        csv_encoding: str = "utf-8-sig",
        excel_engine: str = "openpyxl",
        sheet: Optional[str | int] = 0,
        transform: Optional[Callable[[pd.DataFrame, str], pd.DataFrame]] = None,
        atomic: bool = True,
    ) -> None:
        # <repo> を推定
        self.base_dir = base_dir or Path(__file__).resolve().parents[2]

        # ディレクトリ
        self.excel_dir = self.base_dir / "excel_templates"
        self.raw_dir = self.base_dir / "data" / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # ファイル対応表
        self.mapping = mapping or {
            "input_mail.xlsx": "mail_data_latest.csv",
            "input_staff.xlsx": "staff_data_latest.csv",
        }

        # オプション
        self.csv_encoding = csv_encoding
        self.excel_engine = excel_engine
        self.sheet = sheet
        self.transform = transform
        self.atomic = atomic

        LOGGER.info("ExcelExtractor initialized. base_dir=%s", self.base_dir)

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #
    def extract_file(self, excel_name: str) -> Path:
        """
        指定した Excel を読み込み、CSV に変換して保存します。

        Parameters
        ----------
        excel_name : str
            self.mapping のキーにある Excel ファイル名。

        Returns
        -------
        Path
            保存先 CSV のフルパス
        """
        src = self._excel_path(excel_name)
        if not src.exists():
            LOGGER.error("Excel ファイルが見つかりません: %s", src)
            raise FileNotFoundError(src)

        if excel_name not in self.mapping:
            raise KeyError(f"mapping に未登録のファイルです: {excel_name!r}")

        # 読み込み（エンジン/シートを明示）
        df = pd.read_excel(src, engine=self.excel_engine, sheet_name=self.sheet)
        LOGGER.info("Loaded: %s (shape=%s)", src.name, getattr(df, "shape", None))

        # 任意の加工（列名正規化/型変換など）
        if self.transform is not None:
            df = self.transform(df, excel_name)

        # CSV 保存
        dst = self._csv_path(excel_name)
        self._to_csv_atomic(df, dst)
        LOGGER.info("Saved CSV: %s", dst)
        return dst

    def extract_all(self) -> dict[str, Path]:
        """
        mapping に登録された **全 Excel** を CSV 化して保存します。

        Returns
        -------
        dict[str, Path]
            {excel_name: 保存先CSVパス}
        """
        results: dict[str, Path] = {}
        for name in self.mapping:
            results[name] = self.extract_file(name)
        return results

    # --- 互換: 旧テストが呼ぶ run_all を残す（将来削除予定） ---
    def run_all(self) -> dict[str, Path]:
        """Deprecated alias for extract_all()."""
        return self.extract_all()

    # --------------------------------------------------------------------- #
    # Internals
    # --------------------------------------------------------------------- #
    def _excel_path(self, excel_name: str) -> Path:
        return self.excel_dir / excel_name

    def _csv_path(self, excel_name: str) -> Path:
        return self.raw_dir / self.mapping[excel_name]

    def _to_csv_atomic(self, df: pd.DataFrame, dst: Path) -> None:
        """
        アトミックに CSV を保存（テンポラリ→置換）。`atomic=False` の場合は通常保存。
        """
        if not self.atomic:
            df.to_csv(dst, index=False, encoding=self.csv_encoding)
            return

        tmp = dst.with_suffix(dst.suffix + ".tmp")
        # 既存の .tmp を消してから書く（古い一時ファイルが残っても安全に）
        try:
            if tmp.exists():
                tmp.unlink()
            df.to_csv(tmp, index=False, encoding=self.csv_encoding)
            tmp.replace(dst)  # 同一FS上でアトミックに置換
        finally:
            if tmp.exists():
                # 例外時に一時ファイルが残っていれば掃除
                try:
                    tmp.unlink()
                except Exception:
                    pass


__all__ = ["ExcelExtractor"]
