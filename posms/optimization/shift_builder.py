#!/usr/bin/env python3
"""
posms.optimization.shift_builder
================================
Excel から呼び出して「分担表案」だけを作る（PuLPで最適化）。
仕上げ（案→分担表／勤務指定表）は別マクロ側で実施。

【Excel前提】
- 予測: シート '予測' に '日付'(必須), '通常郵便'(任意), 'is_weekend_or_holiday'(任意)
- 班（スキル表）: 既定は最初のシート or team_sheet='班'
  * 列: '氏名' とジョブ列（0/1 等で担当可。0=事前指定のみ として扱う従来仕様を踏襲）
- 分担表案: A1 にヘッダ (shift_date | emp_id | 備考), A2 から本体を書き込む

使い方（Excel から）:
- xlwings のボタン/RunPython に `作成_分担表案()` を割当
"""

from __future__ import annotations

import os
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional, List, Iterable

import pandas as pd
import pulp
import jpholiday
import xlwings as xw
from datetime import date, datetime

LOGGER = logging.getLogger("posms.optimization.shift_builder")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------- Excel 定数 ----------------
SHT_PRED = "予測"
SHT_DRAFT = "分担表案"

# ---------------- モデル用マッピング ----------------
SHIFT_TYPES = ['早番', '日勤', '夜勤', '通し']
REST_TYPES = [
    '週休', '非番', '祝休','計年', '年休', '夏休', '冬休', '代休',
    '承欠', '休職', '産休', '育休', '介護', '病休', 'その他'
]
SPECIAL_ATT = ['廃休', 'マル超']

# 曜日・祝日別の基礎需要（シフト→業務セット）
DEFAULT_REQ_MAPPING = {
    "Weekday": {
        '早番': {'1区','4区','大口1','大口1-3'},
        '日勤': {'2区','3区','組立','補助'},
        '夜勤': {'速夜'},
        '通し': set(),
    },
    "Saturday": {
        '早番': {'速早'},
        '日勤': {'土曜補助'},
        '夜勤': set(),
        '通し': {'通し'},
    },
    "Sunday": {
        '早番': {'速早'},
        '日勤': set(),
        '夜勤': set(),
        '通し': {'通し'},
    },
    "Holiday": {
        '早番': {'速早'},
        '日勤': set(),
        '夜勤': set(),
        '通し': {'通し'},
    },
}


def get_day_type(d: date) -> str:
    """日付を Weekday / Saturday / Sunday / Holiday に分類"""
    wd = d.weekday()
    if wd == 5:
        return 'Saturday'
    elif wd == 6:
        return 'Sunday'
    elif jpholiday.is_holiday(d):
        return 'Holiday'
    else:
        return 'Weekday'


class ShiftBuilder:
    def __init__(
        self,
        excel_file_path: str,
        team_sheet: Optional[str] = None,
        req_mapping: Optional[Dict[str, Dict[str, set]]] = None,
    ):
        self.excel_file_path = excel_file_path
        self.team_sheet = team_sheet
        self.req_mapping = req_mapping or DEFAULT_REQ_MAPPING

        # 予測データ
        self.pred_df: pd.DataFrame = pd.DataFrame()

        # スキル表（班）
        self.df: pd.DataFrame = pd.DataFrame()

        # 軸
        self.days: List[date] = []
        self.week: List[List[date]] = []
        self.employees: List[str] = []
        self.shift_types: List[str] = SHIFT_TYPES
        self.jobs: List[str] = []

        # 休日系列
        self.saturday: List[date] = []
        self.sunday: List[date] = []
        self.holiday: List[date] = []
        self.weekday: List[date] = []

        # スキル辞書
        self.assignable_jobs: Dict[str, Dict[str, float]] = {}
        self.pre_assigned_jobs: Dict[str, Dict[str, float]] = {}

        # 需要 (日付×シフト×業務) -> 1/0
        self.req: Dict[Tuple[date, str, str], int] = {}

        # スラック/優先度
        self.devPos: Dict[Tuple[str, int], pulp.LpVariable] = {}
        self.devNeg: Dict[Tuple[str, int], pulp.LpVariable] = {}
        self.priority_map: Dict[Tuple[date, str], float] = {}

        # 決定変数
        self.x = None            # x[(i, d, t, k)]
        self.y = None            # y[(i, d)]
        self.x_special = None    # x_special[(i, d, s)]
        self.rest_vars = None    # rest_vars[(i, d, r)]
        self.missing = None      # missing[(d, t, k)]
        self.normal_work = None  # normal_work[(i, d)]

        # 事前指定（無ければ空で動く）
        self.pre_dict_work: Dict[Tuple[str, date, str, str], int] = {}
        self.pre_dict_rest: Dict[Tuple[str, date], str] = {}
        self.pre_dict_special: Dict[Tuple[str, date], str] = {}

        # 数理モデル
        self.model = pulp.LpProblem("Shift_Scheduling", pulp.LpMinimize)

        # データ読込
        self.load_excel_data()

    # ---------------- 読み込み＆前処理 ----------------
    def load_excel_data(self):
        # 1) 予測（'予測' シート）: 必須は '日付'
        try:
            pred = pd.read_excel(self.excel_file_path, sheet_name=SHT_PRED)
        except Exception as e:
            raise FileNotFoundError(f"予測シート '{SHT_PRED}' の読込に失敗: {e}")

        # 日付列名の候補
        date_cols = [c for c in pred.columns if str(c).strip() in ["日付", "date", "Date", "年月日", "yyyymmdd"]]
        if not date_cols:
            raise KeyError("予測シートに '日付' 列（または date/年月日/yyyymmdd）が見つかりません。")
        pred = pred.rename(columns={date_cols[0]: "日付"})
        pred["日付"] = pd.to_datetime(pred["日付"])
        pred = pred.dropna(subset=["日付"]).sort_values("日付").set_index("日付")

        # 週末・祝日フラグ無ければ生成
        if "is_weekend_or_holiday" not in pred.columns:
            pred["is_weekend_or_holiday"] = pred.index.date.map(
                lambda d: 1 if (d.weekday() >= 5 or jpholiday.is_holiday(d)) else 0
            )

        self.pred_df = pred.copy()
        self.days = [d.date() for d in self.pred_df.index]
        if not self.days:
            raise ValueError("予測シートから日付が取得できませんでした。")

        # 2) スキル表（班）: 指定があればそのシート、無ければ最初のシート
        xls = pd.ExcelFile(self.excel_file_path)
        sheet_to_use = self.team_sheet
        if sheet_to_use is None:
            # 予測シート以外の先頭を採用
            other_sheets = [s for s in xls.sheet_names if s != SHT_PRED]
            if not other_sheets:
                raise ValueError("スキル表のシートが見つかりません。")
            sheet_to_use = other_sheets[0]

        # header=1（2行目ヘッダ）を踏襲。必要に応じて 0 に変更してください。
        self.df = pd.read_excel(self.excel_file_path, sheet_name=sheet_to_use, header=1)

        if "氏名" not in self.df.columns:
            raise KeyError(f"スキル表シート '{sheet_to_use}' に '氏名' 列がありません。")

        # ジョブ列（例: 2列目〜16列目を従来通りスキャン。過剰分は自動クリップ）
        self.jobs = list(self.df.columns[1:16])
        if not self.jobs:
            raise ValueError("ジョブ列が見つかりません（'氏名' の右側に 1 列以上必要）。")

        # 社員リスト
        self.employees = [str(x) for x in self.df["氏名"].dropna().astype(str).tolist()]

        # ソルバーが選べる業務: 値>0 を担当可として辞書化
        cols_assign = list(self.df.columns[1:min(13, len(self.df.columns))])
        subset_df = self.df[["氏名"] + cols_assign].copy()
        self.assignable_jobs = (
            subset_df.set_index("氏名")
            .apply(lambda row: {k: v for k, v in row.items() if pd.to_numeric(v, errors="coerce") and float(v) > 0}, axis=1)
            .to_dict()
        )

        # 事前指定のみの業務: 値==0 を「事前指定があれば可」として辞書化（従来仕様）
        cols_pre = list(self.df.columns[1:min(16, len(self.df.columns))])
        subset_df2 = self.df[["氏名"] + cols_pre].copy()
        self.pre_assigned_jobs = (
            subset_df2.set_index("氏名")
            .apply(lambda row: {k: v for k, v in row.items() if pd.to_numeric(v, errors="coerce") == 0}, axis=1)
            .to_dict()
        )

        # 週分割
        self.week = [self.days[i:i + 7] for i in range(0, len(self.days), 7)]

        # 曜日/祝日振り分け
        self.saturday, self.sunday, self.holiday, self.weekday = [], [], [], []
        for d in self.days:
            dt = get_day_type(d)
            if dt == "Saturday":
                self.saturday.append(d)
            elif dt == "Sunday":
                self.sunday.append(d)
            elif dt == "Holiday":
                self.holiday.append(d)
            else:
                self.weekday.append(d)

        # 需要テーブル
        self.set_req()

    # ---------------- 決定変数 ----------------
    def define_variables(self):
        # x[i,d,t,k]：社員 i が日 d のシフト t で業務 k を担当
        self.x = pulp.LpVariable.dicts(
            "Shift",
            [(i, d, t, k) for i in self.employees for d in self.days for t in self.shift_types for k in self.jobs],
            cat='Binary'
        )
        # y[i,d]：社員 i が日 d に勤務
        self.y = pulp.LpVariable.dicts(
            "WorkDay",
            [(i, d) for i in self.employees for d in self.days],
            cat='Binary'
        )
        # 特別状態
        self.x_special = pulp.LpVariable.dicts(
            "x_special",
            [(i, d, s) for i in self.employees for d in self.days for s in SPECIAL_ATT],
            cat='Binary'
        )
        # 休みタイプ
        self.rest_vars = pulp.LpVariable.dicts(
            "Rest",
            [(i, d, r) for i in self.employees for d in self.days for r in REST_TYPES],
            cat='Binary'
        )
        # 欠区（(d,t,k)が未充足）
        self.missing = pulp.LpVariable.dicts(
            "Missing",
            [(d, t, k) for (d, t, k) in self.req.keys()],
            cat='Binary'
        )
        # 連勤判定補助
        self.normal_work = pulp.LpVariable.dicts(
            "NormalWork",
            [(i, d) for i in self.employees for d in self.days],
            cat='Binary'
        )

    # ---------------- 需要設定 ----------------
    def set_req(self):
        self.req = {}
        # マッピングのジョブ名が班シートに無いと x 変数が定義されず整合が取れないため、存在するジョブに限定
        jobs_set = set(self.jobs)

        for d in self.days:
            dt = get_day_type(d)
            if dt not in self.req_mapping:
                continue
            for t in self.shift_types:
                areas: Iterable[str] = self.req_mapping[dt].get(t, set())
                for k in areas:
                    if k in jobs_set:
                        self.req[(d, t, k)] = 1
                    else:
                        LOGGER.warning("需要ジョブ '%s' が班シートの列に存在しないためスキップ（%s, %s）。", k, d, t)

    # ---------------- 制約まとめ ----------------
    def add_constraints(self):
        self.add_rest_constraints()
        self.add_special_constraints()
        self.add_work_constraints()
        self.add_work_or_rest_constraints()
        self.add_one_shift_per_workday()
        self.add_assignability_constraints()
        self.add_holiday_constraints()
        self.add_continuous_work_constraints()
        self.calculate_priority_map()
        self.setup_coverage_and_objective()

    # ---- 休暇事前指定 ----
    def add_rest_constraints(self):
        for i in self.employees:
            for d in self.days:
                if (i, d) in self.pre_dict_rest:
                    r_type = self.pre_dict_rest[(i, d)]
                    # 指定休暇を 1 に固定、他を 0、勤務/特別状態は 0
                    self.model += self.rest_vars[(i, d, r_type)] == 1, f"FixRest_{i}_{d}_{r_type}"
                    for r in REST_TYPES:
                        if r != r_type:
                            self.model += self.rest_vars[(i, d, r)] == 0, f"ZeroRest_{i}_{d}_{r}"
                    for t in self.shift_types:
                        for k in self.jobs:
                            self.model += self.x[(i, d, t, k)] == 0, f"ZeroWork_{i}_{d}_{t}_{k}"
                    for s in SPECIAL_ATT:
                        self.model += self.x_special[(i, d, s)] == 0, f"NoSpecial_{i}_{d}_{s}"
                else:
                    # 指定が無い休暇のうち、計年等は使わせない
                    for r in REST_TYPES:
                        if r not in ["週休", "非番", "祝休"]:
                            self.model += self.rest_vars[(i, d, r)] == 0, f"ZeroRest_{i}_{d}_{r}"

    # ---- 特別状態（廃休・マル超） ----
    def add_special_constraints(self):
        for i in self.employees:
            for d in self.days:
                if (i, d) in self.pre_dict_special:
                    s_type = self.pre_dict_special[(i, d)]
                    for s in SPECIAL_ATT:
                        self.model += self.x_special[(i, d, s)] == (1 if s == s_type else 0), f"Spec_{i}_{d}_{s}"
                else:
                    for s in SPECIAL_ATT:
                        self.model += self.x_special[(i, d, s)] == 0, f"NoSpec_{i}_{d}_{s}"

    # ---- 業務の事前指定/需要反映 ----
    def add_work_constraints(self):
        for i in self.employees:
            for d in self.days:
                for t in self.shift_types:
                    for k in self.jobs:
                        if (i, d, t, k) in self.pre_dict_work:
                            self.model += self.x[(i, d, t, k)] == 1, f"ForceWork_{i}_{d}_{t}_{k}"
                        else:
                            if (d, t, k) not in self.req:
                                self.model += self.x[(i, d, t, k)] == 0, f"NoReq_{i}_{d}_{t}_{k}"
                            # 要求がある組はソルバーに任せる

    # ---- 出勤か休暇か ----
    def add_work_or_rest_constraints(self):
        for i in self.employees:
            for d in self.days:
                self.model += (
                    self.y[(i, d)] + pulp.lpSum(self.rest_vars[(i, d, r)] for r in REST_TYPES)
                ) == 1, f"WorkOrRest_{i}_{d}"

    # ---- 1 日 1 シフト ----
    def add_one_shift_per_workday(self):
        for i in self.employees:
            for d in self.days:
                sum_x_id = pulp.lpSum(self.x[(i, d, t, k)] for t in self.shift_types for k in self.jobs)
                self.model += sum_x_id == self.y[(i, d)], f"OneShiftOrNone_{i}_{d}"

    # ---- スキル適合 ----
    def add_assignability_constraints(self):
        for i in self.employees:
            for d in self.days:
                for t in self.shift_types:
                    for k in self.jobs:
                        if k in self.assignable_jobs.get(i, {}):
                            if (i, d, t, k) in self.pre_dict_work:
                                self.model += self.x[(i, d, t, k)] == 1, f"MustWork_{i}_{d}_{t}_{k}"
                            # else: 自由
                        elif k in self.pre_assigned_jobs.get(i, {}):
                            if (i, d, t, k) in self.pre_dict_work:
                                self.model += self.x[(i, d, t, k)] == 1, f"MustWork_{i}_{d}_{t}_{k}"
                            else:
                                self.model += self.x[(i, d, t, k)] == 0, f"NoWork_{i}_{d}_{t}_{k}"
                        else:
                            self.model += self.x[(i, d, t, k)] == 0, f"InvalidJob_{i}_{d}_{t}_{k}"

    # ---- 休暇の業務ルール ----
    def add_holiday_constraints(self):
        # 週休/非番 と 特別状態の関係
        for i in self.employees:
            for d in self.days:
                self.model += self.rest_vars[(i, d, '週休')] + self.x_special[(i, d, '廃休')] <= 1
                self.model += self.rest_vars[(i, d, '非番')] + self.x_special[(i, d, 'マル超')] <= 1

        # 月次回数
        for i in self.employees:
            self.model += (
                pulp.lpSum(self.rest_vars[(i, d, '週休')] + self.x_special[(i, d, '廃休')] for d in self.days) == 4
            ), f"WeeklyOff_{i}"
            self.model += (
                pulp.lpSum(self.rest_vars[(i, d, '非番')] + self.x_special[(i, d, 'マル超')] for d in self.days) >= 4
            ), f"Hiban_{i}"

        # 週 1 回の週休（スラックあり）
        for i in self.employees:
            for w, days_in_week in enumerate(self.week):
                self.model += (
                    pulp.lpSum(self.rest_vars[(i, d, '週休')] + self.x_special[(i, d, '廃休')] for d in days_in_week) == 1
                ), f"WeeklyOneOff_{i}_{w}"
                self.devPos[(i, w)] = pulp.LpVariable(f"devPos_{i}_{w}", lowBound=0)
                self.devNeg[(i, w)] = pulp.LpVariable(f"devNeg_{i}_{w}", lowBound=0)
                sumHibanOrMarucho = pulp.lpSum(
                    self.rest_vars[(i, d, '非番')] + self.x_special[(i, d, 'マル超')] for d in days_in_week
                )
                self.model += sumHibanOrMarucho - 1 <= self.devPos[(i, w)]
                self.model += 1 - sumHibanOrMarucho <= self.devNeg[(i, w)]

        # 日曜は 週休 or 出勤
        for i in self.employees:
            for d in self.sunday:
                self.model += self.y[(i, d)] + self.rest_vars[(i, d, '週休')] == 1, f"SundayWorkOrOff_{i}_{d}"

        # 土曜は 非番 or 出勤（ただし事前休暇指定があればスキップ）
        for i in self.employees:
            for d in self.saturday:
                if (i, d) in self.pre_dict_rest:
                    continue
                self.model += self.y[(i, d)] + self.rest_vars[(i, d, '非番')] == 1, f"SaturdayWorkOrHiban_{i}_{d}"

        # 祝日以外での祝休は禁止
        for i in self.employees:
            for d in self.days:
                if d not in self.holiday:
                    self.model += self.rest_vars[(i, d, '祝休')] == 0, f"NoHolidayRest_{i}_{d}"

        # 祝日は 出勤 or 祝休（非番を事前指定していればその限りではない）
        for d in self.holiday:
            for i in self.employees:
                if (i, d) in self.pre_dict_rest and self.pre_dict_rest[(i, d)] == '非番':
                    continue
                self.model += self.y[(i, d)] + self.rest_vars[(i, d, '祝休')] == 1, f"HolidayWorkOrHolidayRest_{i}_{d}"

    # ---- 連勤制約 ----
    def add_continuous_work_constraints(self):
        # 10連勤禁止（廃休/マル超があれば最大9）
        for i in self.employees:
            for d_idx in range(len(self.days) - 10 + 1):
                consecutive = [self.days[d_idx + off] for off in range(10)]
                self.model += pulp.lpSum(self.y[(i, dd)] for dd in consecutive) <= 9, f"No10Consecutive_{i}_{d_idx}"

        # normal_work の紐付け
        for i in self.employees:
            for d in self.days:
                self.model += self.normal_work[(i, d)] >= self.y[(i, d)] - self.x_special[(i, d, '廃休')] - self.x_special[(i, d, 'マル超')]
                self.model += self.normal_work[(i, d)] <= 1 - self.x_special[(i, d, '廃休')]
                self.model += self.normal_work[(i, d)] <= 1 - self.x_special[(i, d, 'マル超')]

        # 廃休/マル超なしの連勤は最大5
        for i in self.employees:
            for start_idx in range(len(self.days) - 6 + 1):
                consecutive = [self.days[start_idx + r] for r in range(6)]
                self.model += pulp.lpSum(self.normal_work[(i, d_)] for d_ in consecutive) <= 5, f"No6Normal_{i}_{start_idx}"

    # ---- 出勤可能数と優先度 ----
    def calculate_supply_and_demand(self):
        employee_possible_days = {}

        for i in self.employees:
            base = 20  # 28日 - 週休8（目安）
            extra_rest = 0
            for d in self.days:
                if (i, d) in self.pre_dict_rest and self.pre_dict_rest[(i, d)] in [
                    '計年','年休','夏休','冬休','代休','承欠','休職','産休','育休','介護','病休','その他'
                ]:
                    extra_rest += 1
            employee_possible_days[i] = base - extra_rest

            # 廃休/マル超指定があれば +1
            for d in self.days:
                if (i, d) in self.pre_dict_special and self.pre_dict_special[(i, d)] in ['廃休','マル超']:
                    employee_possible_days[i] += 1

        # 祝日数
        num_holiday = len(self.holiday)

        # 供給合計
        total_supply = sum(employee_possible_days.values())
        total_supply_adj = total_supply - (len(self.employees) - 2) * num_holiday

        # 簡易需要（従来ロジックを踏襲）
        base_demand = 200 - 20
        total_demand = base_demand - (7 * num_holiday)

        excess = total_supply_adj - total_demand

        LOGGER.info("祝日=%d, demand(base)=%d, supply=%d (adj=%d), 過不足=%d",
                    num_holiday, total_demand, total_supply, total_supply_adj, excess)
        return excess

    def calculate_priority_map(self):
        excess_or_def = self.calculate_supply_and_demand()

        base_priority = {
            '1区':10, '2区':10, '3区':10, '4区':10,
            '大口1':10, '大口1-3':10,
            '組立':1, '補助':1,
            '速夜':5, '土曜補助':10,
            '速早':10, '通し':10
        }
        for d in self.days:
            for k, val in base_priority.items():
                self.priority_map[(d, k)] = val

        # 予測に基づく微調整（列があれば）
        if "通常郵便" in self.pred_df.columns:
            if excess_or_def > 0:
                # 余裕がある -> 物数が多い日に補助の優先度を上げる
                top_days = self.pred_df.nlargest(min(excess_or_def, len(self.pred_df)), '通常郵便')
                for ts in top_days.index:
                    dd = ts.date()
                    if (dd, '補助') in self.priority_map:
                        self.priority_map[(dd, '補助')] += 1
            elif excess_or_def < 0:
                # 逼迫 -> 平日の物数が少ない日に速夜の優先度を下げる
                if "is_weekend_or_holiday" in self.pred_df.columns:
                    df_wd = self.pred_df[self.pred_df["is_weekend_or_holiday"] == 0]
                else:
                    df_wd = self.pred_df[[ts.weekday() < 5 for ts in self.pred_df.index]]
                need = min(abs(excess_or_def), len(df_wd))
                for ts in df_wd.nsmallest(need, '通常郵便').index:
                    dd = ts.date()
                    if (dd, '速夜') in self.priority_map:
                        self.priority_map[(dd, '速夜')] = max(0, self.priority_map[(dd, '速夜')] - 2)

    # ---- カバレッジ制約と目的 ----
    def setup_coverage_and_objective(self):
        # 需要カバー: 各 (d,t,k) は 1 人割当 or 欠区
        for (d, t, k), needed in self.req.items():
            self.model += (
                pulp.lpSum(self.x[(i, d, t, k)] for i in self.employees)
                == needed * (1 - self.missing[(d, t, k)])
            ), f"Coverage_{d}_{t}_{k}"

        # 目的関数：欠区の重み + 週1非番ズレのスラック最小化
        obj_missing = pulp.lpSum(
            self.priority_map.get((d, k), 0) * self.missing[(d, t, k)]
            for (d, t, k) in self.req.keys()
        )
        obj_weekdev = pulp.lpSum(
            self.devPos[(i, w)] + self.devNeg[(i, w)]
            for i in self.employees
            for w, _days in enumerate(self.week)
        )
        # 軽い正則化：総出勤人数も軽く抑制（公平寄り）
        obj_total_work = 0.001 * pulp.lpSum(self.y[(i, d)] for i in self.employees for d in self.days)

        self.model += obj_missing + obj_weekdev + obj_total_work

    # ---------------- 解く ----------------
    def solve(self, solver: Optional[pulp.LpSolver] = None) -> str:
        if solver is None:
            solver = pulp.PULP_CBC_CMD(msg=False)
        result = self.model.solve(solver)
        status_str = pulp.LpStatus[self.model.status]
        LOGGER.info("Solve status: %s", status_str)
        return status_str

    # ---------------- “案” の抽出＆書き出し ----------------
    def _collect_draft_rows(self) -> List[Dict[str, object]]:
        """
        “出勤扱い”の社員を (shift_date, emp_id, 備考='案') で列挙。
        出勤扱いの定義：y==1 もしくは 特別状態(廃休/マル超) が 1
        """
        rows = []
        for d in self.days:
            for i in self.employees:
                yv = self.y[(i, d)].varValue if self.y[(i, d)] is not None else 0
                s1 = self.x_special[(i, d, '廃休')].varValue if self.x_special[(i, d, '廃休')] is not None else 0
                s2 = self.x_special[(i, d, 'マル超')].varValue if self.x_special[(i, d, 'マル超')] is not None else 0
                if (yv == 1) or (s1 == 1) or (s2 == 1):
                    rows.append({"shift_date": d, "emp_id": i, "備考": "案"})
        # ソート・重複排除
        if rows:
            df = pd.DataFrame(rows).drop_duplicates().sort_values(["shift_date", "emp_id"])
            return df.to_dict(orient="records")
        return rows

    @staticmethod
    def _clear_below(sht: xw.Sheet, top_left: str = "A2") -> None:
        last = sht.used_range.last_cell
        sht.range(top_left, last).clear_contents()

    def write_draft_to_excel(self, wb: xw.Book) -> Path:
        """caller ブックの '分担表案' に A2 から本体を書き込み、保存パスを返す"""
        rows = self._collect_draft_rows()
        sht = wb.sheets[SHT_DRAFT]
        self._clear_below(sht, "A2")
        if rows:
            df_out = pd.DataFrame(rows)[["shift_date", "emp_id", "備考"]]
            # Excel 表示のため日付は文字列化（テンプレ側の表示形式を使うなら下行を外す）
            df_out["shift_date"] = pd.to_datetime(df_out["shift_date"]).dt.date.astype(str)
            sht["A2"].options(index=False, header=False).value = df_out.values
        wb.save()
        return Path(wb.fullname)

    # ---------------- ワンショット実行 ----------------
    def build_draft(self) -> pd.DataFrame:
        """ユニットテスト等で：案の DataFrame を直接返す"""
        self.define_variables()
        self.add_constraints()
        self.solve()
        return pd.DataFrame(self._collect_draft_rows())


# ---------------- xlwings マクロ（Excel ボタン/RunPython 用） ----------------
@xw.sub
def 作成_分担表案():
    """
    Excel から：予測/班を読み込み → 最適化 → 「分担表案」に出力
    ボタンに割り当てて使用
    """
    wb = xw.Book.caller()
    excel_path = wb.fullname
    if not excel_path:
        raise RuntimeError("このマクロは保存済みのブックから実行してください。")
    builder = ShiftBuilder(excel_file_path=excel_path)
    builder.define_variables()
    builder.add_constraints()
    status = builder.solve()
    if status != "Optimal":
        raise RuntimeError(f"最適化に失敗しました（{status}）")
    builder.write_draft_to_excel(wb)


# ---------------- CLI テスト ----------------
if __name__ == "__main__":
    # Excel ファイル（予測・班が入ったテンプレの実体）を指定
    path = os.environ.get("SHIFT_TEMPLATE", "excel_templates/shift_template.xlsx")
    sb = ShiftBuilder(path)
    df_draft = sb.build_draft()
    print(df_draft.head())
