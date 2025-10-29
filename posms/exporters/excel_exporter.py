from __future__ import annotations
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd
from openpyxl import load_workbook, Workbook

def write_dataframe_to_excel(
    df: pd.DataFrame,
    out_path: Path,
    sheet_name: str = "export",
    template_path: Optional[Path] = None,
    header_map: Optional[Dict[str, str]] = None,
    start_cell: str = "A1",
    append: bool = False,
) -> None:
    """
    DataFrameをテンプレ(任意)へ「値のみ」で書き込み → out_pathに保存する。
    - 数式/外部接続/マクロなし
    - header_map で英⇒日ヘッダ変換可（例: {'name':'氏名','team_name':'班'}）
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if header_map:
        df = df.rename(columns=header_map)
    
    # ★ .xlsm のときは keep_vba=True で読込（VBA温存）
    def _load(path: Path):
        if path.suffix.lower() == ".xlsm":
            return load_workbook(path, keep_vba=True)
        return load_workbook(path)

    if append and out_path.exists():
        wb = load_workbook(out_path)
    else:
        if template_path and Path(template_path).exists():
            wb = load_workbook(template_path)
        else:
            wb = Workbook()
            default_sheet = wb.active
            wb.remove(default_sheet)

    # 既存シートがあれば一度削除（常にクリーンに出す）
    #if sheet_name in wb.sheetnames:
        #ws_existing = wb[sheet_name]
        #wb.remove(ws_existing)

    ws = wb.create_sheet(title=sheet_name)

    # A1 起点にヘッダ＋値を書き込み（型はopenpyxlに任せる）
    # start_cell は "A1" 形式のみ対応
    col_offset = ord(start_cell[0].upper()) - ord("A")
    row_offset = int(start_cell[1:]) - 1

    # ヘッダ
    for j, col in enumerate(df.columns, start=1 + col_offset):
        ws.cell(row=1 + row_offset, column=j, value=str(col))

    # 値
    for i, row in enumerate(df.itertuples(index=False), start=2 + row_offset):
        for j, value in enumerate(row, start=1 + col_offset):
            ws.cell(row=i, column=j, value=value)

    # ざっくり列幅自動（文字数ベース）
    for j, col in enumerate(df.columns, start=1 + col_offset):
        max_len = max([len(str(col))] + [len(str(v)) if v is not None else 0 for v in df.iloc[:, j-1-col_offset]])
        ws.column_dimensions[chr(ord("A") + j - 1)].width = min(max(max_len + 2, 10), 40)

    wb.save(out_path)
