# posms/optimization/shift_builder_grid_solver.py
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import pprint

import pandas as pd
import jpholiday
import pulp


# =========================
#  ユーティリティ
# =========================
weekday_map = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金", 5: "土", 6: "日"}


def normalize_row_kind(x: str) -> str | None:
    s = str(x).strip().lower()
    if s in ("上段", "upper", "u", "top"):
        return "upper"
    if s in ("下段", "lower", "l", "bottom"):
        return "lower"
    return None


def is_sun_holiday(d) -> bool:
    dt_ = pd.Timestamp(d).date()
    return (dt_.weekday() == 6) and jpholiday.is_holiday(dt_)


def is_new_year(d) -> bool:
    dt_ = pd.Timestamp(d).date()
    return (dt_.month == 1) and (dt_.day in (1, 2, 3))  # 必要なら範囲調整


@dataclass
class ShiftBuilderGrid:
    """
    CSVから読み込み -> PuLPで最適化して割当を作る（solver層）

    方針:
      - 互換レイヤーなし（I/D/W/K などは属性としては持たない）
      - 命名は employees/days/week/jobs に統一
      - 制約・目的関数などロジックは元コードのまま
      - A/B/C は意味が伝わらないので「説明的な名前」に置換（ロジック不変）
      - 旧Excel版の「予測に基づく priority 調整」を復活（priority_map を補正）
    """
    csv_dir: Path

    # 予測CSV（任意）。None の場合は csv_dir 内を自動探索。
    forecast_csv: Path | str | None = None

    # ------------ 読み込みDF ------------
    df_emp: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)
    df_zone: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)
    df_need: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)
    df_ft: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)
    df_pt: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)
    df_meta: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)
    df_leave: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)
    df_pre_raw: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)
    df_special_marks: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)

    # 予測DF（任意）
    df_forecast: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False)

    # ------------ 集合/辞書 ------------
    employees: list = field(default_factory=list, init=False)        # 社員番号list
    days: list = field(default_factory=list, init=False)             # date list
    week: list = field(default_factory=list, init=False)             # List[List[date]]

    jobs: list = field(default_factory=list, init=False)             # 全業務（指定含む）
    selectable_jobs: list = field(default_factory=list, init=False)  # ソルバーが選べる業務

    rest_types: list = field(default_factory=list, init=False)
    rest_types_not_count: set = field(default_factory=set, init=False)
    rest_types_count: list = field(default_factory=list, init=False)

    special_attendance: list = field(default_factory=list, init=False)

    emp_working_hours: dict = field(default_factory=dict, init=False)
    emp_dict: dict = field(default_factory=dict, init=False)
    emp_to_code: dict = field(default_factory=dict, init=False)

    # --- A/B/C の改名（ロジック不変）---
    # A: 社員×業務の「重み」（>0 のものだけ）
    emp_job_weight: dict = field(default_factory=dict, init=False)
    # B: 社員×業務の「事前指定のみ許可」（==0 のものだけ）
    emp_job_preassign_only: dict = field(default_factory=dict, init=False)
    # C: 社員×曜日ラベルの「勤務可」（1 のみ）
    emp_day_availability: dict = field(default_factory=dict, init=False)

    # calendar / requirement
    date_label: dict = field(default_factory=dict, init=False)
    mapping: dict = field(default_factory=dict, init=False)
    req: dict = field(default_factory=dict, init=False)
    req_holiday: dict = field(default_factory=dict, init=False)

    # pre-assign
    pre_dict_rest: dict = field(default_factory=dict, init=False)
    pre_dict_work: dict = field(default_factory=dict, init=False)
    pre_dict_special: dict = field(default_factory=dict, init=False)
    pre_dict_time: dict = field(default_factory=dict, init=False)

    # 分類
    sunday: list = field(default_factory=list, init=False)
    holiday: list = field(default_factory=list, init=False)    # 日曜除外の祝日（元ロジックのまま）
    saturday: list = field(default_factory=list, init=False)
    weekday: list = field(default_factory=list, init=False)

    # その他
    zone_to_shift: dict = field(default_factory=dict, init=False)
    avail: dict = field(default_factory=dict, init=False)

    job_availability: dict = field(default_factory=dict, init=False)
    priority_map: dict = field(default_factory=dict, init=False)
    missing_zone_priority: list = field(default_factory=list, init=False)

    # ------------ PuLP 変数 ------------
    model: pulp.LpProblem | None = field(default=None, init=False)
    x: dict = field(default_factory=dict, init=False)
    y: dict = field(default_factory=dict, init=False)
    rest: dict = field(default_factory=dict, init=False)
    specialWork: dict = field(default_factory=dict, init=False)
    missing: dict = field(default_factory=dict, init=False)
    normal_work: dict = field(default_factory=dict, init=False)
    devPos: dict = field(default_factory=dict, init=False)
    devNeg: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.csv_dir = Path(self.csv_dir)
        if self.forecast_csv is not None:
            self.forecast_csv = Path(self.forecast_csv)

    # =========================
    #  1) CSV 読み込み
    # =========================
    def load_csvs(self) -> None:
        cd = self.csv_dir
        self.df_emp  = pd.read_csv(cd / "employees.csv", encoding="cp932")
        self.df_zone = pd.read_csv(cd / "zones.csv", encoding="cp932")
        self.df_need = pd.read_csv(cd / "employee_demand.csv", encoding="cp932")
        self.df_ft   = pd.read_csv(cd / "jobtype_fulltime.csv", encoding="cp932")
        self.df_pt   = pd.read_csv(cd / "jobtype_parttime.csv", encoding="cp932")
        self.df_meta = pd.read_csv(cd / "shift_meta.csv", encoding="cp932")
        self.df_leave = pd.read_csv(cd / "leave_types.csv", encoding="cp932")
        self.df_pre_raw = pd.read_csv(cd / "pre_assignments.csv", encoding="cp932")

        sp_path = cd / "special_marks.csv"
        if sp_path.exists():
            self.df_special_marks = pd.read_csv(sp_path, encoding="cp932")
        else:
            self.df_special_marks = pd.DataFrame({"emp_no": [], "date": [], "kind": []})

    # =========================
    #  予測CSV（任意）
    # =========================
    def _load_forecast_csv(self) -> None:
        """
        物数予測CSVを任意で読み込む。
        無ければ df_forecast は空のまま（＝従来通り）。
        """
        candidates: list[Path] = []

        # 明示指定があれば優先
        if self.forecast_csv is not None:
            candidates.append(Path(self.forecast_csv))

        # 自動探索（よくある命名）
        candidates += [
            self.csv_dir / "forecast.csv",
            self.csv_dir / "prediction.csv",
            self.csv_dir / "predictions.csv",
            self.csv_dir / "予測.csv",
        ]

        fp = next((p for p in candidates if p.exists()), None)
        if fp is None:
            self.df_forecast = pd.DataFrame()
            return

        try:
            df = pd.read_csv(fp, encoding="cp932")
        except Exception:
            df = pd.read_csv(fp, encoding="utf-8-sig")

        date_cols = [c for c in df.columns if str(c).strip() in ["日付", "date", "Date", "年月日", "yyyymmdd"]]
        if not date_cols:
            self.df_forecast = pd.DataFrame()
            return

        df = df.rename(columns={date_cols[0]: "日付"})
        df["日付"] = pd.to_datetime(df["日付"], errors="coerce")
        df = df.dropna(subset=["日付"]).copy()
        df["日付"] = df["日付"].dt.date

        if self.days:
            day_set = set(self.days)
            df = df[df["日付"].isin(day_set)].copy()

        if "is_weekend_or_holiday" not in df.columns:
            df["is_weekend_or_holiday"] = df["日付"].map(
                lambda d: 1 if (d.weekday() >= 5 or jpholiday.is_holiday(d)) else 0
            )

        self.df_forecast = df.set_index("日付").sort_index()

    # =========================
    #  2) meta（shift_meta.csv の日付が正）
    # =========================
    def _build_meta(self) -> None:
        self.start_date = self.df_meta["start_date"].iloc[0]
        self.end_date   = self.df_meta["end_date"].iloc[0]

        self.days = (
            pd.to_datetime(self.df_meta["日付"])
            .dt.normalize()
            .dt.date
            .tolist()
        )

        # 週ごとに分割（7日単位）
        self.week = [self.days[i:i+7] for i in range(0, len(self.days), 7)]

        weekday = {d: d.weekday() for d in self.days}
        is_holiday = {d: jpholiday.is_holiday(d) for d in self.days}

        self.sunday   = [d for d in self.days if weekday[d] == 6]
        self.holiday  = [d for d in self.days if is_holiday[d] and weekday[d] != 6]  # 日曜除外（元ロジックのまま）
        self.saturday = [d for d in self.days if weekday[d] == 5 and not is_holiday[d]]
        self.weekday  = [d for d in self.days if d not in self.sunday and d not in self.holiday and d not in self.saturday]

    # =========================
    #  3) 社員/業務/休暇などの基本集合・辞書
    # =========================
    def _build_sets(self) -> None:
        df_emp = self.df_emp
        df_zone = self.df_zone
        df_need = self.df_need
        df_leave = self.df_leave

        self.emp_working_hours = (
            df_emp.set_index("社員番号")[["勤務時間(日)", "勤務時間(月)"]]
            .to_dict(orient="index")
        )
        self.emp_dict = (
            df_emp.set_index("社員番号")[["氏名", "社員タイプ"]]
            .to_dict(orient="index")
        )

        self.employees = list(df_emp["社員番号"])

        # 全業務 jobs（指定業務含む）
        self.jobs = df_zone.loc[df_zone["稼働"].isin(["通配", "混合", "組立", "指定業務"]), "区名"].tolist()

        # ソルバーが選べる業務（指定業務除外）
        self.selectable_jobs = df_zone.loc[df_zone["稼働"].isin(["通配", "混合", "組立"]), "区名"].tolist()

        # 区名 -> シフトタイプ
        self.zone_to_shift = df_zone.set_index("区名")["シフトタイプ"].to_dict()

        # 社員番号 -> {早番/日勤/中勤/夜勤}
        shift_cols = ["早番", "日勤", "中勤", "夜勤"]
        self.emp_to_code = df_need.set_index("社員番号")[shift_cols].to_dict(orient="index")

        # 休暇集合
        if "leave_name" in df_leave.columns:
            self.rest_types = list(df_leave["leave_name"])
        else:
            self.rest_types = list(df_leave["休暇名"])

        self.rest_types_not_count = {"非番", "週休"}
        self.rest_types_count = [r for r in self.rest_types if r not in self.rest_types_not_count]

        self.special_attendance = ["廃休", "マル超"]

    # =========================
    #  4) 社員×業務 / 社員×曜日 の可否マップ
    # =========================
    def _build_ability_maps(self) -> None:
        df_need = self.df_need

        # emp_job_weight（ソルバーが選べる：>0）
        selected_columns = ["社員番号"] + self.selectable_jobs
        subset_df = df_need[selected_columns]
        self.emp_job_weight = (
            subset_df.set_index("社員番号")
            .apply(lambda row: {k: v for k, v in row.items() if v > 0}, axis=1)
            .to_dict()
        )

        # emp_job_preassign_only（指定のみ：==0）
        selected_columns = ["社員番号"] + self.jobs
        subset_df = df_need[selected_columns]
        self.emp_job_preassign_only = (
            subset_df.set_index("社員番号")
            .apply(lambda row: {k: v for k, v in row.items() if v == 0}, axis=1)
            .to_dict()
        )

        # emp_day_availability（曜日可否：1/0）
        day_cols = ["月", "火", "水", "木", "金", "土", "日", "祝"]
        selected_columns = ["社員番号"] + day_cols
        subset_df = df_need[selected_columns]
        self.emp_day_availability = (
            subset_df.set_index("社員番号")
            .apply(lambda row: {k: int(v) for k, v in row.items() if int(v) == 1}, axis=1)
            .to_dict()
        )

    # =========================
    #  5) date_label / mapping / req
    # =========================
    def _build_calendar_maps(self) -> None:
        # date_label
        self.date_label = {}
        for d in self.days:
            wd = d.weekday()
            if wd == 6:
                self.date_label[d] = "日"
            elif jpholiday.is_holiday(d):
                self.date_label[d] = "祝"
            else:
                self.date_label[d] = weekday_map[wd]

        # df_zone の曜日稼働 mapping（曜日->区リスト）
        day_cols = ["月", "火", "水", "木", "金", "土", "日", "祝"]
        z = self.df_zone.copy()
        for c in day_cols:
            if c in z.columns:
                z[c] = pd.to_numeric(z[c], errors="coerce").fillna(0)
        self.mapping = {
            day: z.loc[z[day] >= 1, "区名"].tolist()
            for day in day_cols
            if day in z.columns
        }

        # req（元ロジックのまま）
        self.req = {}
        for d in self.days:
            wd = d.weekday()
            is_hol = jpholiday.is_holiday(d)

            if wd == 6:
                label = "日"
            elif is_hol:
                label = "祝"
            elif wd == 5:
                label = "土"
            else:
                label = ["月", "火", "水", "木", "金"][wd]

            for k in self.mapping.get(label, []):
                self.req[(d, k)] = 1

        self.req_holiday = {(d, k): v for (d, k), v in self.req.items() if d in set(self.holiday)}

    # =========================
    #  6) pre_assignments を辞書化
    # =========================
    def _build_pre_dicts(self) -> None:
        df_pre_raw = self.df_pre_raw

        required_pre_cols = ["emp_no", "date", "row_kind", "value"]
        missing_cols = [c for c in required_pre_cols if c not in df_pre_raw.columns]
        if missing_cols:
            raise KeyError(f"pre_assignments.csv missing columns: {missing_cols} / have: {df_pre_raw.columns.tolist()}")

        df_pre = df_pre_raw[required_pre_cols].copy()
        df_pre["date"] = pd.to_datetime(df_pre["date"]).dt.date
        df_pre["emp_no"] = pd.to_numeric(df_pre["emp_no"], errors="raise").astype(int)

        D_set = set(self.days)
        before = len(df_pre)
        df_pre = df_pre[df_pre["date"].isin(D_set)].copy()
        after = len(df_pre)
        print("df_pre rows: before filter =", before, "after filter =", after)

        # leave_codes / zone_codes
        df_leave = self.df_leave
        if "leave_name" in df_leave.columns:
            leave_codes = set(df_leave["leave_name"].astype(str).str.strip().tolist())
        elif "休暇名" in df_leave.columns:
            leave_codes = set(df_leave["休暇名"].astype(str).str.strip().tolist())
        else:
            raise KeyError(f"leave_types.csv columns not supported: {df_leave.columns.tolist()}")

        if "区名" in self.df_zone.columns:
            zone_codes = set(self.df_zone["区名"].astype(str).str.strip().tolist())
        else:
            zone_codes = set()

        pre_list_special = ["廃休", "マル超"]

        self.pre_dict_rest = {}
        self.pre_dict_work = {}
        self.pre_dict_special = {}
        self.pre_dict_time = {}

        unknown_kind = []
        unknown_values = []

        for _, row in df_pre.iterrows():
            emp_no = int(row["emp_no"])
            date_key = row["date"]  # date
            kind = normalize_row_kind(row["row_kind"])
            val = row["value"]

            if pd.isna(val) or str(val).strip() == "":
                continue
            val_str = str(val).strip()

            if kind is None:
                unknown_kind.append((emp_no, date_key, str(row["row_kind"]), val_str))
                continue

            if kind == "upper":
                self.pre_dict_time[(emp_no, date_key)] = val_str
            else:
                if val_str in pre_list_special:
                    self.pre_dict_special[(emp_no, date_key)] = val_str
                elif val_str in leave_codes:
                    self.pre_dict_rest[(emp_no, date_key)] = val_str
                elif val_str in zone_codes:
                    self.pre_dict_work[(emp_no, date_key)] = val_str
                else:
                    unknown_values.append((emp_no, date_key, val_str))

        # special_marks.csv（任意）で special を上書き
        df_special_marks = self.df_special_marks
        if df_special_marks is not None and len(df_special_marks) > 0:
            required_sp_cols = ["emp_no", "date", "kind"]
            sp_missing = [c for c in required_sp_cols if c not in df_special_marks.columns]
            if sp_missing:
                raise KeyError(f"special_marks.csv missing columns: {sp_missing} / have: {df_special_marks.columns.tolist()}")

            df_special = df_special_marks[required_sp_cols].copy()
            df_special["date"] = pd.to_datetime(df_special["date"]).dt.date
            df_special["emp_no"] = pd.to_numeric(df_special["emp_no"], errors="raise").astype(int)

            D_set = set(self.days)
            df_special = df_special[df_special["date"].isin(D_set)].copy()

            for _, row in df_special.iterrows():
                emp_no = int(row["emp_no"])
                date_key = row["date"]
                kind = str(row["kind"]).strip()
                if kind in pre_list_special:
                    self.pre_dict_special[(emp_no, date_key)] = kind

        print("\n[DEBUG] contains '計年' in leave_codes?:", "計年" in leave_codes)
        print("[DEBUG] contains '非番' in leave_codes?:", "非番" in leave_codes)
        if unknown_kind:
            print("\n[WARN] unknown row_kind examples (first 10):")
            pprint.pprint(unknown_kind[:10])
        if unknown_values:
            print("\n[WARN] value not classified examples (first 20):")
            pprint.pprint(unknown_values[:20])
            print("  -> leave_types.csv / zones.csv との表記不一致の可能性")

    # =========================
    #  7) 祝日ルール用：祝休置換と禁止
    # =========================
    def _apply_shukkyu_normalization(self) -> None:
        def allow_shukkyu_on_sun_holiday(i, d) -> bool:
            return (self.emp_dict[i]["社員タイプ"] == "正社員") and is_new_year(d)

        def shukkyu_forbidden(i, d) -> bool:
            return is_sun_holiday(d) and (not allow_shukkyu_on_sun_holiday(i, d))

        self.allow_shukkyu_on_sun_holiday = allow_shukkyu_on_sun_holiday
        self.shukkyu_forbidden = shukkyu_forbidden

        pre_dict_rest_norm = dict(self.pre_dict_rest)
        for (i, d), r in list(pre_dict_rest_norm.items()):
            if r == "祝休" and shukkyu_forbidden(i, d):
                pre_dict_rest_norm[(i, d)] = "週休"
        self.pre_dict_rest = pre_dict_rest_norm

    # =========================
    #  予測ロジック（旧Excel版のまま）
    # =========================
    def _calculate_supply_and_demand_excess(self) -> int:
        """
        旧Excel版の calculate_supply_and_demand() 相当。
        priority調整に使う過不足（excess）を返す。
        """
        extra_rest_codes = {
            "計年", "年休", "夏休", "冬休", "代休",
            "承欠", "休職", "産休", "育休", "介護", "病休", "その他",
        }

        employee_possible_days: dict[int, int] = {}
        for i in self.employees:
            base = 20  # 旧ロジック踏襲
            extra_rest = 0

            for d in self.days:
                r = self.pre_dict_rest.get((i, d))
                if r in extra_rest_codes:
                    extra_rest += 1

            employee_possible_days[i] = base - extra_rest

            for d in self.days:
                s = self.pre_dict_special.get((i, d))
                if s in ("廃休", "マル超"):
                    employee_possible_days[i] += 1

        num_holiday = len(self.holiday)
        total_supply = sum(employee_possible_days.values())
        total_supply_adj = total_supply - (len(self.employees) - 2) * num_holiday  # 旧ロジックそのまま

        base_demand = 200 - 20  # 旧ロジック踏襲
        total_demand = base_demand - (7 * num_holiday)

        excess = total_supply_adj - total_demand
        return int(excess)

    def _apply_forecast_priority_adjustment(self) -> None:
        """
        旧Excel版の calculate_priority_map() の「予測ベース補正」部分を移植。
        df_forecast が空 or 通常郵便が無い場合は何もしない。
        """
        if self.df_forecast is None or self.df_forecast.empty:
            return
        if "通常郵便" not in self.df_forecast.columns:
            return

        excess = self._calculate_supply_and_demand_excess()

        # 余りがある→忙しい日に「補助」の優先度を上げる
        if excess > 0:
            n = min(excess, len(self.df_forecast))
            top_days = self.df_forecast.nlargest(n, "通常郵便")
            for d in top_days.index:
                key = (d, "補助")
                if key in self.priority_map:
                    self.priority_map[key] = float(self.priority_map[key]) + 1.0

        # 足りない→（平日で）暇な日に「速夜」の優先度を下げる
        elif excess < 0:
            df_wd = self.df_forecast[self.df_forecast["is_weekend_or_holiday"] == 0]
            n = min(abs(excess), len(df_wd))
            low_days = df_wd.nsmallest(n, "通常郵便")
            for d in low_days.index:
                key = (d, "速夜")
                if key in self.priority_map:
                    self.priority_map[key] = max(0.0, float(self.priority_map[key]) - 2.0)

    # =========================
    #  8) priority_map / 欠区優先順位
    # =========================
    def _build_priority_maps(self) -> None:
        subset = self.df_zone.loc[
            self.df_zone["区名"].isin(self.selectable_jobs),
            ["区名", "月", "火", "水", "木", "金", "土", "日", "祝"]
        ].set_index("区名")
        self.job_availability = subset.to_dict(orient="index")

        exclude = {"補助", "速早", "速夜", "組立"}
        ja = pd.DataFrame.from_dict(self.job_availability, orient="index")
        dow_cols = ["月", "火", "水", "木", "金", "土", "日", "祝"]
        priority_series = (
            ja.loc[~ja.index.isin(exclude), dow_cols]
              .sum(axis=1)
              .sort_values(ascending=True)
        )
        self.missing_zone_priority = priority_series.index.tolist()

        self.priority_map = {
            (d, k): self.job_availability.get(k, {}).get(self.date_label[d], 0)
            for (d, k) in self.req.keys()
        }

        # ★予測に基づく priority 補正（旧ロジック復活）
        self._apply_forecast_priority_adjustment()

    # =========================
    #  9) avail
    # =========================
    def _build_avail(self) -> None:
        self.avail = {
            (i, d): 1 if (i, d) in self.pre_dict_work else self.emp_day_availability.get(i, {}).get(self.date_label[d], 0)
            for i in self.emp_day_availability
            for d in self.days
        }

    # =========================
    #  10) 変数定義（元ロジックそのまま）
    # =========================
    def _define_variables(self) -> None:
        self.model = pulp.LpProblem("Shift_Scheduling", pulp.LpMinimize)

        self.x = pulp.LpVariable.dicts(
            "Shift",
            [(i, d, k) for i in self.employees for d in self.days for k in self.jobs],
            cat="Binary"
        )

        self.y = pulp.LpVariable.dicts(
            "WorkDay",
            [(i, d) for i in self.employees for d in self.days],
            cat="Binary"
        )

        self.rest = pulp.LpVariable.dicts(
            "Rest",
            [(i, d, r) for i in self.employees for d in self.days for r in self.rest_types],
            cat="Binary"
        )

        self.specialWork = pulp.LpVariable.dicts(
            "X_Special",
            [(i, d, s) for i in self.employees for d in self.days for s in self.special_attendance],
            cat=pulp.LpBinary
        )

        self.missing = pulp.LpVariable.dicts(
            "Missing",
            [(d, k) for (d, k) in self.req.keys()],
            cat="Binary"
        )

        self.normal_work = pulp.LpVariable.dicts(
            "NormalWork",
            [(i, d) for i in self.employees for d in self.days],
            cat=pulp.LpBinary
        )

        self.devPos = {}
        self.devNeg = {}

    # =========================
    #  11) 制約（元ロジックそのまま）
    # =========================
    def add_constraints(self) -> None:
        model = self.model
        assert model is not None

        # ★ここは「数式の形」を守るために残す（ロジック不変）
        I = self.employees
        D = self.days
        K = self.jobs
        R = self.rest_types
        W = self.week

        x, y, rest = self.x, self.y, self.rest
        specialWork, missing = self.specialWork, self.missing
        normal_work = self.normal_work
        devPos, devNeg = self.devPos, self.devNeg
        req = self.req

        job_weight = self.emp_job_weight
        preassign_only = self.emp_job_preassign_only

        pre_dict_rest = self.pre_dict_rest
        pre_dict_work = self.pre_dict_work
        pre_dict_special = self.pre_dict_special
        special_attendance = self.special_attendance
        avail = self.avail
        R_COUNT = self.rest_types_count

        emp_working_hours = self.emp_working_hours
        emp_dict = self.emp_dict

        D_sunday = self.sunday
        D_saturday = self.saturday
        D_holiday = self.holiday

        shukkyu_forbidden = self.shukkyu_forbidden
        allow_shukkyu_on_sun_holiday = self.allow_shukkyu_on_sun_holiday

        # ---- 曜日による出勤指定 ----
        for i in I:
            for d in D:
                for k in K:
                    model += x[(i, d, k)] <= avail.get((i, d), 0), f"Avail_{i}_{d}_{k}"

        # ---- 休暇の事前指定 ----
        for i in I:
            for d in D:
                if (i, d) in pre_dict_rest:
                    rest_type = pre_dict_rest[(i, d)]
                    model += rest[(i, d, rest_type)] == 1, f"Rest_{i}_{d}_{rest_type}"
                    for r_ in R:
                        if r_ != rest_type:
                            model += rest[(i, d, r_)] == 0, f"RestZero_{i}_{d}_{r_}"
                    for k in K:
                        model += x[(i, d, k)] == 0, f"ZeroX_{i}_{d}_{k}"
                    model += y[(i, d)] == 0, f"ZeroY_{i}_{d}"
                    for s in special_attendance:
                        model += specialWork[(i, d, s)] == 0, f"ZeroSpWork_{i}_{d}_{s}"
                else:
                    for r in R:
                        if r not in ["週休", "非番", "祝休"]:
                            model += rest[(i, d, r)] == 0, f"ZeroRest_{i}_{d}_{r}"

        # ---- 廃休、マル超の事前指定 ----
        for i in I:
            for d in D:
                if (i, d) in pre_dict_special:
                    s_type = pre_dict_special[(i, d)]
                    for s in special_attendance:
                        if s == s_type:
                            model += specialWork[(i, d, s)] == 1, f"FixSpec_{i}_{d}_{s}"
                        else:
                            model += specialWork[(i, d, s)] == 0, f"ZeroSpec_{i}_{d}_{s}"
                else:
                    for s in special_attendance:
                        model += specialWork[(i, d, s)] == 0, f"NoSpec_{i}_{d}_{s}"

        # ---- 業務の事前指定（reqに無い区は0固定） ----
        for i in I:
            for d in D:
                for k in K:
                    if pre_dict_work.get((i, d)) == k:
                        model += x[(i, d, k)] == 1, f"ForceWork_{i}_{d}_{k}"
                    else:
                        if (d, k) not in req:
                            model += x[(i, d, k)] == 0, f"NoReq_{i}_{d}_{k}"

        # ---- 割当可否（job_weight / preassign_only / invalid）----
        for i in I:
            for d in D:
                for k in K:
                    if k in job_weight[i]:
                        if pre_dict_work.get((i, d)) == k:
                            model += x[(i, d, k)] == 1, f"MustWork_{i}_{d}_{k}"
                    elif k in preassign_only[i]:
                        if pre_dict_work.get((i, d)) == k:
                            model += x[(i, d, k)] == 1, f"MustWork_{i}_{d}_{k}"
                        else:
                            model += x[(i, d, k)] == 0, f"NoWork_{i}_{d}_{k}"
                    else:
                        model += x[(i, d, k)] == 0, f"InvalidJob_{i}_{d}_{k}"

        # ---- WorkOrRest ----
        for i in I:
            for d in D:
                model += (y[(i, d)] + pulp.lpSum(rest[(i, d, r)] for r in R)) == 1, f"WorkOrRest_{i}_{d}"

        # ---- 1日1シフト ----
        for i in I:
            for d in D:
                model += pulp.lpSum(x[(i, d, k)] for k in K) == y[(i, d)], f"OneShiftOrNone_{i}_{d}"

        # ---- 週休/非番 と 特別の排他 ----
        for i in I:
            for d in D:
                model += rest[(i, d, "週休")] + specialWork[(i, d, "廃休")] <= 1
                model += rest[(i, d, "非番")] + specialWork[(i, d, "マル超")] <= 1

        # ---- 月の回数 ----
        for i in I:
            model += pulp.lpSum(rest[(i, d, "週休")] + specialWork[(i, d, "廃休")] for d in D) == 4, f"WeeklyOff_{i}"
            model += pulp.lpSum(rest[(i, d, "非番")] + specialWork[(i, d, "マル超")] for d in D) >= 4, f"Hiban_{i}"

        # ---- 週に1回週休（廃休カウント） ----
        for i in I:
            for w, days_in_week in enumerate(W):
                model += pulp.lpSum(rest[(i, d, "週休")] + specialWork[(i, d, "廃休")] for d in days_in_week) == 1

        # ---- 週1非番ズレ（スラック） ----
        for i in I:
            for w, days_in_week in enumerate(W):
                sumHibanOrMarucho = pulp.lpSum(
                    rest[(i, d, "非番")] + specialWork[(i, d, "マル超")]
                    for d in days_in_week
                )
                devPos[(i, w)] = pulp.LpVariable(f"devPos_{i}_{w}", lowBound=0)
                devNeg[(i, w)] = pulp.LpVariable(f"devNeg_{i}_{w}", lowBound=0)
                model += sumHibanOrMarucho - 1 <= devPos[(i, w)]
                model += 1 - sumHibanOrMarucho <= devNeg[(i, w)]

        # ---- 日曜：休むなら週休 ----
        for i in I:
            for d in D_sunday:
                model += y[(i, d)] + rest[(i, d, "週休")] == 1, f"SundayWorkOrWeeklyOff_{i}_{d}"

        # ---- 土曜：休むなら非番（事前指定休暇があればスキップ）----
        for i in I:
            for d in D_saturday:
                if (i, d) in pre_dict_rest:
                    continue
                model += y[(i, d)] + rest[(i, d, "非番")] == 1, f"SaturdayWorkOrNonban_{i}_{d}"

        # ---- 月勤務時間（等式） ----
        for i in I:
            day_h = float(emp_working_hours[i]["勤務時間(日)"])
            mon_h = float(emp_working_hours[i]["勤務時間(月)"])

            worked_days = pulp.lpSum(y[(i, d)] for d in D)
            counted_leave_days = pulp.lpSum(rest[(i, d, r)] for d in D for r in R_COUNT)

            model += day_h * (worked_days + counted_leave_days) == mon_h

        # ---- 祝日以外は祝休禁止 ----
        for i in I:
            for d in D:
                if d not in D_holiday:
                    model += rest[(i, d, "祝休")] == 0, f"NoHolidayRest_{i}_{d}"

        # ---- 日曜祝日は祝休禁止（例外あり）----
        for i in I:
            for d in D:
                if shukkyu_forbidden(i, d):
                    model += rest[(i, d, "祝休")] == 0, f"NoShukkyuOnSunHol_{i}_{d}"

        # ---- 祝日のルール（非番の事前指定は尊重）----
        for d in D_holiday:
            for i in I:
                if (i, d) in pre_dict_rest and pre_dict_rest[(i, d)] == "非番":
                    continue

                emp_type = emp_dict[i]["社員タイプ"]

                if is_sun_holiday(d):
                    if allow_shukkyu_on_sun_holiday(i, d):
                        model += y[(i, d)] + rest[(i, d, "祝休")] == 1, f"SunHolNY_Reg_{i}_{d}"
                    else:
                        model += y[(i, d)] + rest[(i, d, "週休")] == 1, f"SunHol_WorkOrShukyu_{i}_{d}"
                    continue

                if emp_type == "正社員":
                    model += y[(i, d)] + rest[(i, d, "祝休")] == 1, f"Hol_Reg_{i}_{d}"
                else:
                    model += y[(i, d)] + rest[(i, d, "非番")] == 1, f"Hol_NonReg_{i}_{d}"
                    model += rest[(i, d, "祝休")] == 0, f"NoShukkyu_NonReg_{i}_{d}"

        # ---- 10連勤禁止（<=9）----
        for i in I:
            for d_idx in range(len(D) - 10 + 1):
                consecutive_days = [D[d_idx + offset] for offset in range(10)]
                model += pulp.lpSum(y[(i, day_)] for day_ in consecutive_days) <= 9, f"No10ConsecutiveDays_{i}_{d_idx}"

        # ---- normal_work 紐付け ----
        for i in I:
            for d in D:
                model += normal_work[(i, d)] >= y[(i, d)] - specialWork[(i, d, "廃休")] - specialWork[(i, d, "マル超")]
                model += normal_work[(i, d)] <= 1 - specialWork[(i, d, "廃休")]
                model += normal_work[(i, d)] <= 1 - specialWork[(i, d, "マル超")]

        # ---- 通常連勤は5まで（6窓で<=5）----
        for i in I:
            for start_idx in range(len(D) - 6 + 1):
                consecutive_dates = [D[start_idx + r] for r in range(6)]
                model += pulp.lpSum(normal_work[(i, d_)] for d_ in consecutive_dates) <= 5

        # ---- 欠区カバレッジ（forced除外）----
        for (d, k), needed in req.items():
            forced_workers = [i for i in I if pre_dict_work.get((i, d)) == k]
            forced_count   = len(forced_workers)
            free_workers = [i for i in I if i not in forced_workers]

            if forced_count > 0:
                model += (missing[(d, k)] == 0), f"MissingFixed0_{d}_{k}"

            model += (
                pulp.lpSum(x[(i, d, k)] for i in free_workers) + forced_count
                == needed * (1 - missing[(d, k)])
            ), f"Coverage_{d}_{k}"

    # =========================
    #  12) 目的関数（元ロジックそのまま）
    # =========================
    def set_objective(self, alpha: float = 0.1) -> None:
        model = self.model
        assert model is not None

        I, D, W = self.employees, self.days, self.week
        req = self.req
        priority_map = self.priority_map
        missing = self.missing
        devPos, devNeg = self.devPos, self.devNeg
        job_weight = self.emp_job_weight
        x = self.x

        obj = (
            pulp.lpSum(
                priority_map.get((d, k), 0) * missing[(d, k)]
                for (d, k) in req.keys()
            )
            + pulp.lpSum(
                devPos[(i, w)] + devNeg[(i, w)]
                for i in I
                for w in range(len(W))
            )
            - alpha * pulp.lpSum(
                job_weight[i][k] * x[(i, d, k)]
                for i in job_weight
                for d in D
                for k in job_weight[i].keys()
            )
        )
        model += obj

    # =========================
    #  実行（パイプライン）
    # =========================
    def build(self) -> None:
        self.load_csvs()
        self._build_meta()

        # ★days が確定したあとで予測を読む（任意）
        self._load_forecast_csv()

        self._build_sets()
        self._build_ability_maps()
        self._build_calendar_maps()
        self._build_pre_dicts()
        self._apply_shukkyu_normalization()
        self._build_avail()
        self._build_priority_maps()
        self._define_variables()
        self.add_constraints()

    def solve(self, alpha: float = 0.1, msg: bool = True):
        self.set_objective(alpha=alpha)
        assert self.model is not None
        status = self.model.solve(pulp.PULP_CBC_CMD(msg=msg))
        self.status = status
        self.status_name = pulp.LpStatus[status]
        return status

    def summary(self) -> dict:
        return {
            "status": getattr(self, "status_name", None),
            "employees": len(self.employees),
            "days": len(self.days),
            "jobs": len(self.jobs),
            "req": len(self.req),
            "forecast_rows": 0 if self.df_forecast is None else len(self.df_forecast),
        }
