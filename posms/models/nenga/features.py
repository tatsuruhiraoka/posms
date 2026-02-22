# posms/models/nenga/features.py
from __future__ import annotations

from dataclasses import dataclass
import pandas as pd
import numpy as np
from sqlalchemy import text
import jpholiday


@dataclass(frozen=True)
class NengaConfig:
    # 年賀組立: 12/26〜1/15 を1
    prep_start_md: tuple[int, int] = (12, 26)
    prep_end_md: tuple[int, int] = (1, 15)
    # 年賀配達: 1/1〜1/15 を1
    deliv_end_day: int = 15


def _holiday_flag(d: pd.Timestamp) -> int:
    # 土日 or 祝日
    return int((d.weekday() >= 5) or bool(jpholiday.is_holiday(d.date())))


def _add_calendar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    d = pd.to_datetime(df["date"])
    df["year"] = d.dt.year
    df["month"] = d.dt.month
    df["day"] = d.dt.day
    df["weekday"] = d.dt.weekday
    df["dayofyear"] = d.dt.dayofyear
    df["holiday"] = d.map(_holiday_flag).astype(int)
    return df


def _add_nenga_flags(df: pd.DataFrame, cfg: NengaConfig) -> pd.DataFrame:
    df = df.copy()
    d = pd.to_datetime(df["date"])

    # 組立フラグ
    is_prep = ((d.dt.month == 12) & (d.dt.day >= 26)) | (
        (d.dt.month == 1) & (d.dt.day <= 15)
    )
    df["is_nenga_prep"] = is_prep.astype(int)

    # 配達フラグ
    is_deliv = (d.dt.month == 1) & (d.dt.day <= cfg.deliv_end_day)
    df["is_nenga_delivery"] = is_deliv.astype(int)

    # 組立 offset: 12/26->0 ... 12/31->5, 1/1->6 ... 1/15->20, else -1
    def prep_offset(x: pd.Timestamp) -> int:
        if x.month == 12 and x.day >= 26:
            return x.day - 26
        if x.month == 1 and x.day <= 15:
            return 6 + x.day
        return -1

    # 配達 offset: 1/1->0 ... 1/15->14, else -1
    def deliv_offset(x: pd.Timestamp) -> int:
        if x.month == 1 and x.day <= 15:
            return x.day - 1
        return -1

    df["nenga_prep_offset"] = d.map(prep_offset).astype(int)
    df["nenga_delivery_offset"] = d.map(deliv_offset).astype(int)

    # 配達分割用（元日と 1/2-1/15）
    df["is_newyear_day"] = ((d.dt.month == 1) & (d.dt.day == 1)).astype(int)
    df["is_after_newyear"] = (
        (d.dt.month == 1) & (d.dt.day >= 2) & (d.dt.day <= 15)
    ).astype(int)
    df["after_newyear_offset"] = np.where(
        df["is_after_newyear"].eq(1), d.dt.day - 2, -1
    ).astype(int)
    # 1/3 専用フラグ（休日でも配達の特異日）
    df["is_jan3"] = ((d.dt.month == 1) & (d.dt.day == 3)).astype(int)

    return df


def _add_lags(df: pd.DataFrame, target_col: str = "actual_volume") -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values("date").reset_index(drop=True)

    df["lag_1"] = df[target_col].shift(1)
    df["lag_7"] = df[target_col].shift(7)
    # lag_365（前年同日）
    # shift(365) はうるう年でズレるため、日付で前年同日を参照する
    # date 列を Datetime にしておく（念のため）
    df["date"] = pd.to_datetime(df["date"])
    # 前年同日の index を作る
    prev_year_dates = df["date"] - pd.DateOffset(years=1)
    # date → actual_volume の対応表
    s = df.set_index("date")[target_col].astype(float)
    # 前年同日を map
    df["lag_365"] = prev_year_dates.map(s)
    df["lag_730"] = (df["date"] - pd.DateOffset(years=2)).map(s)
    df["lag_1095"] = (df["date"] - pd.DateOffset(years=3)).map(s)
    return df


class NengaFeatureBuilder:
    """
    mailvolume_by_type から (date, office_id, mail_kind) の時系列を読み、
    年賀用特徴量を組み立てる。
    """

    def __init__(
        self, engine, *, office_id: int, mail_kind: str, cfg: NengaConfig | None = None
    ):
        self.engine = engine
        self.office_id = office_id
        self.mail_kind = mail_kind
        self.cfg = cfg or NengaConfig()

    def load(self) -> pd.DataFrame:
        sql = text("""
            SELECT
              date,
              office_id,
              mail_kind,
              actual_volume,
              forecast_volume,
              price_increase_flag
            FROM mailvolume_by_type
            WHERE office_id = :office_id
              AND mail_kind = :mail_kind
            ORDER BY date
        """)
        df = pd.read_sql(
            sql,
            self.engine,
            params={"office_id": self.office_id, "mail_kind": self.mail_kind},
        )
        df["date"] = pd.to_datetime(df["date"])
        return df

    def build(self) -> pd.DataFrame:
        df = self.load()
        df = _add_calendar(df)
        df = _add_nenga_flags(df, self.cfg)
        df = _add_lags(df, "actual_volume")
        return df
