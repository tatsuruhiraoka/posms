"""
posms.excel_app.runner
======================

Excel ⇆ Python ブリッジ（xlwings）

ワークフロー
------------
1. Excel 側でボタン (Assign Macro) → ``posms_xlwings.run_monthly``
2. 本モジュールが Prefect Flow ``monthly_refresh`` を同期呼び
3. 成功なら「完了しました」ダイアログ、失敗はエラー内容を返す
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

import xlwings as xw
from prefect import flow
from posms.flows.monthly_flow import monthly_refresh
from posms.optimization.shift_builder import OutputType

LOGGER = logging.getLogger("posms.excel_app.runner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class XlwingsRunner:
    """Excel から POSMS フローをトリガーする薄いラッパ"""

    def __init__(self) -> None:
        self.wb = xw.Book.caller()  # 呼び出し元のブック

    # -------- private helpers ---------------------------------------
    def _get_parameters(self) -> Dict[str, Any]:
        """Excel シート '設定' からパラメータを取得 (例 A1:A3)"""
        try:
            sht = self.wb.sheets["設定"]
            predict_date = sht.range("B1").value  # YYYY-MM-DD
            output_type = sht.range("B2").value or "分担表"
            return {
                "predict_date": predict_date,
                "output_type": output_type,
                "excel_template": "excel_templates/shift_template.xlsx",
            }
        except (KeyError, AttributeError):
            # フォールバック: 今日の日付
            from datetime import date

            return {"predict_date": str(date.today()), "output_type": "分担表"}

    def _notify(self, msg: str) -> None:
        xw.apps.active.api.MsgBox(msg, 0, "POSMS")

    # -------- macro entrypoints -------------------------------------
    @xw.sub
    def run_monthly(self) -> None:
        """Excel マクロとして登録するエントリポイント"""
        params = self._get_parameters()
        try:
            LOGGER.info("Running monthly_refresh with %s", params)
            # Prefect Flow を同期呼び出し
            monthly_refresh(**params)
            self._notify("POSMS 完了しました。")
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Flow failed: %s", exc)
            self._notify(f"エラーが発生しました: {exc}")

    @xw.func
    def demand_forecast(self, date_str: str) -> int:
        """
        =posms_xlwings.demand_forecast("2025-08-05")
        のようにセル関数として需要予測を返す
        """
        from posms.features import FeatureBuilder
        demand = FeatureBuilder().predict(date_str)
        return int(demand)


# -------- Excel add‑in エントリポイント -----------------------------
# xlwings では、モジュール名を *アドイン名* にして登録すると
# `posms_xlwings.<macro>` がリボンや VBA から呼び出せる
runner = XlwingsRunner()
run_monthly = runner.run_monthly
demand_forecast = runner.demand_forecast
