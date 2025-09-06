# posms/excel_app/runner.py
"""
posms.excel_app.runner
=====================

Excel ⇆ Python ブリッジ（xlwings）

ワークフロー
------------
1. Excel 側でボタン (Assign Macro) → ``posms_xlwings.run_monthly``
2. 本モジュールがフロー ``monthly_refresh`` を同期呼び
3. 成功なら「完了しました」ダイアログ、失敗はエラー内容を返す

方針
----
- ライブラリ側では logging.basicConfig を呼ばない（呼び出し元に委ねる）
- .env には依存しない（ゼロ設定）
- Excel セルの値（datetime/文字列/シリアル数値）の型ゆれを ISO 日付に正規化
- テンプレートのパスは絶対パスに解決（作業ディレクトリに依存しない）
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import xlwings as xw

from posms.flows.monthly_flow import monthly_refresh

LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parents[2]  # <repo>


class XlwingsRunner:
    """Excel から POSMS フローをトリガーする薄いラッパ"""

    def __init__(self) -> None:
        # Excel から呼ばれない場合（テスト/デバッグ）はフォールバック
        try:
            self.wb = xw.Book.caller()
        except Exception:  # Excel 外で実行された場合など
            try:
                self.wb = xw.books.active
            except Exception:
                self.wb = None

    # -------- private helpers ---------------------------------------
    def _get_parameters(self) -> Dict[str, Any]:
        """Excel シート '設定' からパラメータを取得。無ければ安全な既定値にフォールバック。"""
        tpl = str(BASE_DIR / "excel_templates" / "shift_template.xlsx")
        # Excel ブックが無ければ既定値
        if self.wb is None:
            return {
                "predict_date": date.today().isoformat(),
                "output_type": "分担表",
                "excel_template": tpl,
            }

        try:
            sht = self.wb.sheets["設定"]
            predict_raw = sht.range("B1").value  # 日付/文字列/シリアル数値になり得る
            predict_date = self._to_iso_date(predict_raw)
            output_type = str(sht.range("B2").value or "分担表").strip()
            return {
                "predict_date": predict_date,
                "output_type": output_type,
                "excel_template": tpl,
            }
        except (KeyError, AttributeError) as e:
            LOGGER.warning(
                "設定シートの読み取りに失敗したため既定値を使用します: %s", e
            )
            return {
                "predict_date": date.today().isoformat(),
                "output_type": "分担表",
                "excel_template": tpl,
            }
        except Exception as e:  # 予期せぬ型など
            LOGGER.warning("設定値の解釈に失敗したため既定値を使用します: %s", e)
            return {
                "predict_date": date.today().isoformat(),
                "output_type": "分担表",
                "excel_template": tpl,
            }

    def _notify(self, msg: str) -> None:
        """Excel のメッセージボックス。Excel 外では print にフォールバック。"""
        try:
            xw.apps.active.api.MsgBox(msg, 0, "POSMS")
        except Exception:
            print(f"[POSMS] {msg}")

    @staticmethod
    def _to_iso_date(v: Any) -> str:
        """Excelセルの値（datetime/日付文字列/シリアル数値等）を YYYY-MM-DD に正規化。"""
        if isinstance(v, (date, datetime, pd.Timestamp)):
            return pd.to_datetime(v).date().isoformat()
        if isinstance(v, (int, float)):
            # Excel シリアル日付（1900 系）。origin を明示してズレ防止
            return pd.to_datetime(v, unit="D", origin="1899-12-30").date().isoformat()
        # 文字列などは to_datetime に任せる（失敗時は上位でフォールバック）
        return pd.to_datetime(str(v)).date().isoformat()

    # -------- macro entrypoints -------------------------------------
    @xw.sub
    def run_monthly(self) -> None:
        """Excel マクロとして登録するエントリポイント"""
        params = self._get_parameters()
        try:
            LOGGER.info("Running monthly_refresh with %s", params)
            monthly_refresh(**params)
            self._notify("POSMS 完了しました。")
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Flow failed: %s", exc)
            self._notify(f"エラーが発生しました: {exc}")

    @xw.func
    def demand_forecast(self, date_input: Any) -> int:
        """
        セル関数:
            =posms_xlwings.demand_forecast("2025-08-05")
            =posms_xlwings.demand_forecast(A1)   （A1が日付/文字列/シリアルでも可）
        """
        from posms.features import FeatureBuilder

        iso = self._to_iso_date(date_input)
        demand = FeatureBuilder().predict(iso)
        return int(demand)


# -------- Excel add-in エントリポイント -----------------------------
# xlwings では、モジュール名を *アドイン名* にして登録すると
# `posms_xlwings.<macro>` がリボンや VBA から呼び出せる
runner = XlwingsRunner()
run_monthly = runner.run_monthly
demand_forecast = runner.demand_forecast

if __name__ == "__main__":
    # Excel 外での簡易デバッグ用
    # （通知は print にフォールバックし、設定は既定値で実行します）
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    runner.run_monthly()
