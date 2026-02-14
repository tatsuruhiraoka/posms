# posms/features/builder.py
"""
posms.features.builder
======================

FeatureBuilder（mailvolume_by_type + jpholiday 対応 / 固定）

前提（設計固定）:
- テーブルは mailvolume_by_type を使用する（後方互換・自動探索はしない）
- office_id + mail_kind で 1 本の系列を扱う
- 特徴量:
    dow, dow_sin, dow_cos,
    is_holiday, is_after_holiday, is_after_after_holiday,
    month, season (1:春,2:夏,3:秋,4:冬),
    lag_1, lag_7, rolling_mean_7,
    is_new_year, is_obon,
    price_increase_flag
- 学習用 (X, y) と、期間フレーム生成 API を提供（予測は別レイヤ）
"""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional, List

import numpy as np
import pandas as pd
import jpholiday
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

LOGGER = logging.getLogger(__name__)

MAIL_TABLE = "mailvolume_by_type"

FEATURE_COLUMNS: List[str] = [
    "dow",
    "dow_sin",
    "dow_cos",
    "is_holiday",
    "is_after_holiday",
    "is_after_after_holiday",
    "month",
    "season",
    "lag_1",
    "lag_7",
    "rolling_mean_7",
    "is_new_year",
    "is_obon",
    "price_increase_flag",
]


class FeatureBuilder:
    """
    Parameters
    ----------
    office_id : int | None
        対象局 ID。None の場合、データ内に 1 局しか無ければ自動選択。
        複数局が存在するのに未指定なら例外。
    mail_kind : str
        mailvolume_by_type.mail_kind の値（normal, registered_plus, ...）
    base_dir : Path | None
        （将来拡張用）プロジェクトルート推定に使用。
    engine : sqlalchemy.engine.Engine | None
        既存 Engine を渡す場合。None なら環境変数から自動生成。
    """

    def __init__(
        self,
        office_id: Optional[int] = None,
        mail_kind: str = "normal",
        base_dir: Path | None = None,
        engine: Engine | None = None,
    ) -> None:
        self.base_dir = base_dir or Path(__file__).resolve().parents[2]
        self.office_id = office_id
        self.mail_kind = mail_kind
        self.engine = engine or self._make_engine_from_env()

        LOGGER.info(
            "FeatureBuilder initialized. db=%s office_id=%s mail_kind=%s",
            self.engine.url,
            self.office_id,
            self.mail_kind,
        )

    # ------------------------- DB 接続 -------------------------
    def _make_engine_from_env(self) -> Engine:
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            url = database_url
        else:
            user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
            pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            name = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")
            if not all([user, pwd, name]):
                raise RuntimeError(
                    "DB connection info is incomplete. "
                    "Set DATABASE_URL or POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB."
                )
            url = URL.create(
                "postgresql+psycopg2",
                username=user,
                password=pwd,
                host=host,
                port=int(port) if port else None,
                database=name,
            )
        return create_engine(url, pool_pre_ping=True, future=True)

    # ----------------------- データ読み込み ---------------------
    def _load_mail(self) -> pd.DataFrame:
        """
        mailvolume_by_type から系列を読み込み。

        必要列:
          - date
          - office_id
          - mail_kind
          - actual_volume
          - price_increase_flag

        NOTE:
        - office_id が None の場合、mail_kind で絞った結果が 1局だけなら自動選択
          2局以上なら例外
        """
        mk = (self.mail_kind or "normal").lower()

        if self.office_id is not None:
            sql = f"""
                SELECT "date", office_id, actual_volume, price_increase_flag
                FROM {MAIL_TABLE}
                WHERE office_id = :office_id
                  AND mail_kind = :mail_kind
                ORDER BY "date"
            """
            df = pd.read_sql(
                text(sql),
                self.engine,
                params={"office_id": self.office_id, "mail_kind": mk},
                parse_dates=["date"],
            )
        else:
            sql = f"""
                SELECT "date", office_id, actual_volume, price_increase_flag
                FROM {MAIL_TABLE}
                WHERE mail_kind = :mail_kind
                ORDER BY office_id, "date"
            """
            df = pd.read_sql(
                text(sql),
                self.engine,
                params={"mail_kind": mk},
                parse_dates=["date"],
            )

            n_offices = df["office_id"].nunique() if not df.empty else 0
            if n_offices == 0:
                raise ValueError(
                    f"{MAIL_TABLE}(mail_kind={mk}) が空です。データを投入してください。"
                )
            if n_offices > 1:
                raise ValueError(
                    "office_id を指定してください（複数局のデータが存在します）。"
                )
            self.office_id = int(df["office_id"].iloc[0])
            df = df[df["office_id"] == self.office_id].copy()

        if df.empty:
            raise ValueError(
                f"{MAIL_TABLE} に office_id={self.office_id}, mail_kind={mk} のデータがありません。"
            )

        # price_increase_flag を 0/1 に正規化（欠損は 0）
        if "price_increase_flag" in df.columns:
            df["price_increase_flag"] = (
                df["price_increase_flag"].fillna(False).astype(bool).astype(int)
            )
        else:
            df["price_increase_flag"] = 0

        df = df.sort_values("date").reset_index(drop=True)
        return df

    # ----------------------- 祝日（jpholiday） -----------------
    def _is_holiday_series(self, dates: pd.Series) -> pd.Series:
        """
        祝日判定 + 週末を含めた「非稼働日」。
        戻り値: 0/1 int Series（name='is_holiday' として返す）
        """
        dates = pd.to_datetime(dates)
        is_pub_holiday = dates.dt.date.map(lambda d: bool(jpholiday.is_holiday(d))).astype(
            int
        )
        is_weekend = (dates.dt.weekday >= 5).astype(int)
        non_working = ((is_pub_holiday == 1) | (is_weekend == 1)).astype(int)
        return non_working.rename("is_holiday")

    # ----------------------- 特徴量生成 ------------------------
    @staticmethod
    def _assign_season(ts: pd.Timestamp) -> int:
        """1: 春 (3–5), 2: 夏 (6–8), 3: 秋 (9–11), 4: 冬 (12–2)"""
        m = int(ts.month)
        if m in (3, 4, 5):
            return 1
        if m in (6, 7, 8):
            return 2
        if m in (9, 10, 11):
            return 3
        return 4

    @staticmethod
    def _is_new_year(ts: pd.Timestamp) -> int:
        """正月（1/1-1/3）"""
        return int(ts.month == 1 and 1 <= ts.day <= 3)

    @staticmethod
    def _is_obon(ts: pd.Timestamp) -> int:
        """お盆（8/13-8/16）"""
        return int(ts.month == 8 and 13 <= ts.day <= 16)

    def _add_features(self, df: pd.DataFrame, *, y_col: str = "actual_volume") -> pd.DataFrame:
        """
        df に特徴量列を追加して返す。

        y_col:
            lag/rolling 計算の元となる系列列。
            将来、予測値で埋めた列（例：y_filled）を渡せるようにするために可変化している。
        """
        df = df.copy()

        # dow & cyclic encoding
        df["dow"] = df["date"].dt.weekday
        df["dow_sin"] = np.sin(2 * np.pi * df["dow"] / 7.0)
        df["dow_cos"] = np.cos(2 * np.pi * df["dow"] / 7.0)

        # month & season
        df["month"] = df["date"].dt.month
        df["season"] = df["date"].apply(self._assign_season).astype(int)

        # holiday flags（jpholiday + weekend）
        # NOTE: rolling で _add_features を繰り返し呼んでも衝突しないように join ではなく代入にする
        hol = self._is_holiday_series(df["date"]).to_numpy()
        df["is_holiday"] = hol
        df["is_after_holiday"] = pd.Series(df["is_holiday"]).shift(1, fill_value=0).astype(int).to_numpy()
        df["is_after_after_holiday"] = pd.Series(df["is_holiday"]).shift(2, fill_value=0).astype(int).to_numpy()

        # event flags
        df["is_new_year"] = df["date"].apply(self._is_new_year).astype(int)
        df["is_obon"] = df["date"].apply(self._is_obon).astype(int)

        # lags & rolling
        if y_col not in df.columns:
            raise ValueError(f"y_col={y_col!r} not found in df columns={list(df.columns)}")

        df["lag_1"] = df[y_col].shift(1)
        df["lag_7"] = df[y_col].shift(7)
        df["rolling_mean_7"] = df[y_col].shift(1).rolling(7).mean()

        # 未来行などで列欠けが起きても落とさない保険
        for c in FEATURE_COLUMNS:
            if c not in df.columns:
                df[c] = 0

        return df

    def _features_df(self, *, dropna: bool) -> pd.DataFrame:
        base = self._load_mail()
        out = self._add_features(base, y_col="actual_volume")
        if dropna:
            # 学習では特徴量の欠損行を落とし、目的変数も欠損なしに限定
            out = out.dropna(subset=FEATURE_COLUMNS + ["actual_volume"])
        return out.reset_index(drop=True)

    # ----------------------- 期間フレーム生成 ---------------------
    def build_frame(
        self,
        date_min: str | date,
        date_max: str | date,
        *,
        include_future: bool = True,
        y_col: str = "actual_volume",
    ) -> pd.DataFrame:
        """
        指定期間 [date_min, date_max] の「日次連番」フレームを返す。

        - DB に存在しない日付も、include_future=True なら行として生成する
          （actual_volume は NaN、price_increase_flag は 0）
        - lag/rolling の元系列は y_col で指定（将来、予測値で埋めた列を使える）
        """
        base = self._load_mail()

        d1 = pd.to_datetime(str(date_min)).normalize()
        d2 = pd.to_datetime(str(date_max)).normalize()
        if d2 < d1:
            raise ValueError(f"date_max must be >= date_min: {d1.date()}..{d2.date()}")

        # 期間で切り出し（実績がある範囲は保持）
        base = base[(base["date"] >= d1) & (base["date"] <= d2)].copy()

        if include_future:
            idx = pd.date_range(d1, d2, freq="D")
            base = base.set_index("date").reindex(idx)
            base.index.name = "date"
            base = base.reset_index()

            # office_id を埋める
            if "office_id" not in base.columns or base["office_id"].isna().all():
                base["office_id"] = self.office_id
            else:
                base["office_id"] = base["office_id"].ffill().bfill().fillna(self.office_id)

            # 未来日の price_increase_flag は 0 扱い
            if "price_increase_flag" not in base.columns:
                base["price_increase_flag"] = 0
            base["price_increase_flag"] = base["price_increase_flag"].fillna(0).astype(int)

        if y_col not in base.columns:
            raise ValueError(
                f"y_col={y_col!r} not found in frame columns={list(base.columns)}"
            )

        out = self._add_features(base, y_col=y_col)
        return out.reset_index(drop=True)

    # ------------------------- 外部 API -------------------------
    def build(self) -> tuple[pd.DataFrame, pd.Series]:
        """
        学習データを返す。

        Returns
        -------
        X : pandas.DataFrame（FEATURE_COLUMNS の順序で固定）
        y : pandas.Series（actual_volume）
        """
        df = self._features_df(dropna=True)
        X = df[FEATURE_COLUMNS].astype(float)
        y = df["actual_volume"].astype(float)
        LOGGER.info("Feature matrix built: rows=%d, cols=%d", *X.shape)
        return X, y

    def build_with_dates(self, *, dropna: bool = True) -> pd.DataFrame:
        """
        解析/デバッグ用に、日付・目的変数・特徴量を含む DataFrame を返す。
        dropna=False にすると特徴量欠損の行も残す（ただし _load_mail は実績のみなので通常は少ない）
        """
        df = self._features_df(dropna=dropna)
        cols = ["date", "office_id", "actual_volume"] + FEATURE_COLUMNS
        return df[cols]

    # NOTE: predict() は廃止（予測は別レイヤに移す）
    def predict(self, *args, **kwargs) -> float:  # pragma: no cover
        raise RuntimeError(
            "FeatureBuilder.predict() is deprecated. "
            "Use rolling forecast pipeline (forecast_4weeks / ModelPredictor) instead."
        )
