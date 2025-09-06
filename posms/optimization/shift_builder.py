#!/usr/bin/env python3
"""
posms.optimization.shift_builder
================================

需要 Series と社員 DataFrame を入力に
PuLP でシフト最適化 → Excel 3 シートへ出力

Excel テンプレート
------------------
excel_templates/shift_template.xlsx

* シート名
  - 分担表          : 確定版
  - 勤務指定表      : 社員 × 日付 × 時間帯 マトリクス
  - 分担表案        : 草案（"案" ラベル付き）
"""

from __future__ import annotations

import logging
from enum import Enum
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import xlwings as xw
from pulp import LpBinary, LpMinimize, LpProblem, LpVariable, lpSum, PULP_CBC_CMD

LOGGER = logging.getLogger("posms.optimization.shift_builder")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class OutputType(str, Enum):
    分担表 = "分担表"
    勤務指定表 = "勤務指定表"
    分担表案 = "分担表案"


class ShiftBuilder:
    """
    Parameters
    ----------
    template_path : Path
        3 シート入り Excel テンプレート
    """

    def __init__(self, template_path: Path) -> None:
        if not template_path.exists():
            raise FileNotFoundError(template_path)
        self.template = template_path

    # ------------------------------------------------------------------
    # 1. 最適化ロジック
    # ------------------------------------------------------------------
    def _optimize(self, demand: pd.Series, staff: pd.DataFrame) -> pd.DataFrame:
        """
        demand : Index=yyyymmdd, value=mail_count (int)
        staff  : columns=[emp_id, capacity]
        """
        prob = LpProblem("ShiftOptimization", LpMinimize)

        # 変数 x[date, emp] 0/1
        x: Dict[Tuple, LpVariable] = {
            (d, e): LpVariable(f"x_{d}_{e}", 0, 1, cat=LpBinary)
            for d in demand.index
            for e in staff["emp_id"]
        }

        # 目的: 総シフト数を最小化
        prob += lpSum(x.values())

        # 制約: 需要充足
        for d in demand.index:
            prob += (
                lpSum(
                    x[d, e] * staff.set_index("emp_id").loc[e, "capacity"]
                    for e in staff["emp_id"]
                )
                >= demand[d]
            )

        prob.solve(PULP_CBC_CMD(msg=False))

        # 結果整形
        rows = [
            {"shift_date": d, "emp_id": e}
            for (d, e), var in x.items()
            if var.value() == 1
        ]
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # 2. Excel Writers
    # ------------------------------------------------------------------
    def _write_assignment(self, sht, df: pd.DataFrame) -> None:
        df_out = df.sort_values(["shift_date", "emp_id"])
        sht["A2"].options(index=False).value = df_out

    def _write_shift_spec(self, sht, df: pd.DataFrame, staff: pd.DataFrame) -> None:
        pivot = (
            df.assign(v=1)
            .pivot(index="emp_id", columns="shift_date", values="v")
            .reindex(index=staff["emp_id"])
            .fillna("")
        )
        sht["A2"].options(index=True, header=True).value = pivot

    def _write_assignment_draft(self, sht, df: pd.DataFrame) -> None:
        df_draft = df.copy()
        df_draft["備考"] = "案"
        self._write_assignment(sht, df_draft)

    # ------------------------------------------------------------------
    # 3. Public API
    # ------------------------------------------------------------------
    def build(
        self,
        demand: pd.Series,
        staff: pd.DataFrame,
        output_type: OutputType = OutputType.分担表,
        save_path: Path | None = None,
    ) -> Path:
        """
        Returns
        -------
        Path : 保存先 Excel パス
        """
        df_shift = self._optimize(demand, staff)

        wb = xw.Book(self.template)
        match output_type:
            case OutputType.分担表:
                self._write_assignment(wb.sheets["分担表"], df_shift)
            case OutputType.勤務指定表:
                self._write_shift_spec(wb.sheets["勤務指定表"], df_shift, staff)
            case OutputType.分担表案:
                self._write_assignment_draft(wb.sheets["分担表案"], df_shift)
            case _:
                raise ValueError(output_type)

        out = save_path or Path(f"outputs/{output_type.value}_output.xlsx")
        out.parent.mkdir(exist_ok=True, parents=True)
        wb.save(out)
        wb.close()
        LOGGER.info("Excel saved → %s", out.resolve())
        return out


# ---------------- CLI テスト ----------------
if __name__ == "__main__":
    # ダミーデータで動作確認
    dates = pd.date_range("2025-08-01", periods=5, freq="D")
    demand = pd.Series([100, 120, 90, 110, 80], index=dates.date)

    staff = pd.DataFrame({"emp_id": [1, 2, 3, 4, 5], "capacity": [30, 25, 20, 25, 30]})

    tpl = Path("excel_templates/shift_template.xlsx")
    ShiftBuilder(tpl).build(demand, staff, OutputType.分担表案)
