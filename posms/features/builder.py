# posms/features/builder.py
"""
posms.features.builder
======================

FeatureBuilder（MailVolume + jpholiday 対応）

- 単一 office_id の系列を DB から取得（DATABASE_URL か POSTGRES_* で接続）
- 特徴量:
    dow, dow_sin, dow_cos,
    is_holiday, is_after_holiday, is_after_after_holiday,
    month, season (1:春,2:夏,3:秋,4:冬),
    lag_1, lag_7, rolling_mean_7,
    is_new_year, is_obon,
    price_increase_flag
- 学習用 (X, y) と、単一日の予測 API を提供
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
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import URL, Engine

from posms.models import ModelPredictor

LOGGER = logging.getLogger(__name__)

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
        対象の郵便種別。mailvolume_by_type.mail_kind の値（normal, yu_packet, ...）。
        旧 MailVolume テーブルを使う場合は無視される。
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

    # ----------------------- テーブル解決 ----------------------
    def _resolve_mail_table_name(self) -> str:
        """
        実在テーブル名を自動解決（大文字/小文字/別名に頑健）。

        優先順位:
        - mailvolume_by_type / "MailVolumeByType"
        - mailvolume / "MailVolume" / mail_volume
        """
        insp = inspect(self.engine)

        # SQLite と Postgres で get_table_names の呼び方を変える
        if insp.dialect.name == "sqlite":
            existing = set(insp.get_table_names())
        else:
            # Postgres 等（public スキーマ）
            existing = set(insp.get_table_names(schema="public"))

        def norm(s: str) -> str:
            return s.replace('"', "").lower()

        candidates = [
            "mailvolume_by_type",
            '"MailVolumeByType"',
            "mailvolume",
            '"MailVolume"',
            "mail_volume_by_type",
            "mail_volume",
        ]
        existing_norm = {t.lower() for t in existing}
        for c in candidates:
            if norm(c) in existing_norm:
                return c
        raise RuntimeError(
            f"MailVolume/ByType テーブルが見つかりません。存在テーブル={sorted(existing)} / 期待候補={candidates}"
        )

    # ----------------------- データ読み込み ---------------------
    def _load_mail(self) -> pd.DataFrame:
        """
        MailVolume / MailVolumeByType から系列を読み込み。
        必要列: date, office_id, actual_volume, price_increase_flag
        """
        tbl = self._resolve_mail_table_name()
        tbl_norm = tbl.replace('"', "").lower()

        if "mailvolume_by_type" in tbl_norm:
            # ---- 種別別テーブル（mailvolume_by_type） ----
            if self.office_id is not None:
                sql = f"""
                    SELECT "date", office_id, actual_volume, price_increase_flag
                    FROM {tbl}
                    WHERE office_id = :office_id
                      AND mail_kind = :mail_kind
                    ORDER BY "date"
                """
                df = pd.read_sql(
                    text(sql),
                    self.engine,
                    params={"office_id": self.office_id, "mail_kind": self.mail_kind},
                    parse_dates=["date"],
                )
            else:
                # office_id 未指定時：1局だけなら自動で選ぶ（従来仕様を踏襲）
                sql = f"""
                    SELECT "date", office_id, actual_volume, price_increase_flag
                    FROM {tbl}
                    WHERE mail_kind = :mail_kind
                    ORDER BY office_id, "date"
                """
                df = pd.read_sql(
                    text(sql),
                    self.engine,
                    params={"mail_kind": self.mail_kind},
                    parse_dates=["date"],
                )
                n_offices = df["office_id"].nunique() if not df.empty else 0
                if n_offices == 0:
                    raise ValueError(
                        f"MailVolumeByType(mail_kind={self.mail_kind}) が空です。データを投入してください。"
                    )
                if n_offices > 1:
                    raise ValueError(
                        "office_id を指定してください（複数局のデータが存在します）。"
                    )
                self.office_id = int(df["office_id"].iloc[0])
                df = df[df["office_id"] == self.office_id].copy()
        else:
            # ---- 旧 MailVolume テーブル（後方互換用）----
            if self.office_id is not None:
                sql = f"""
                    SELECT "date", office_id, actual_volume, price_increase_flag
                    FROM {tbl}
                    WHERE office_id = :office_id
                    ORDER BY "date"
                """
                df = pd.read_sql(
                    text(sql),
                    self.engine,
                    params={"office_id": self.office_id},
                    parse_dates=["date"],
                )
            else:
                sql = f"""
                    SELECT "date", office_id, actual_volume, price_increase_flag
                    FROM {tbl}
                    ORDER BY office_id, "date"
                """
                df = pd.read_sql(text(sql), self.engine, parse_dates=["date"])
                n_offices = df["office_id"].nunique() if not df.empty else 0
                if n_offices == 0:
                    raise ValueError("MailVolume が空です。データを投入してください。")
                if n_offices > 1:
                    raise ValueError(
                        "office_id を指定してください（複数局のデータが存在します）。"
                    )
                self.office_id = int(df["office_id"].iloc[0])
                df = df[df["office_id"] == self.office_id].copy()

        if df.empty:
            raise ValueError(
                f"対象テーブル {tbl} に office_id={self.office_id}, "
                f"mail_kind={getattr(self, 'mail_kind', None)} のデータがありません。"
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
        is_pub_holiday = dates.dt.date.map(lambda d: bool(jpholiday.is_holiday(d))).astype(int)
        is_weekend = (dates.dt.weekday >= 5).astype(int)
        non_working = ((is_pub_holiday == 1) | (is_weekend == 1)).astype(int)
        return non_working.rename("is_holiday")

    # ----------------------- 特徴量生成 ------------------------
    @staticmethod
    def _assign_season(ts: pd.Timestamp) -> int:
        """
        1: 春 (3–5), 2: 夏 (6–8), 3: 秋 (9–11), 4: 冬 (12–2)
        """
        m = int(ts.month)
        if m in (3, 4, 5):
            return 1
        elif m in (6, 7, 8):
            return 2
        elif m in (9, 10, 11):
            return 3
        else:
            return 4

    @staticmethod
    def _is_new_year(ts: pd.Timestamp) -> int:
        # 正月（1/1-1/3）
        return int(ts.month == 1 and 1 <= ts.day <= 3)

    @staticmethod
    def _is_obon(ts: pd.Timestamp) -> int:
        # お盆（8/13-8/16）
        return int(ts.month == 8 and 13 <= ts.day <= 16)

    def _add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # dow & cyclic encoding
        df["dow"] = df["date"].dt.weekday
        df["dow_sin"] = np.sin(2 * np.pi * df["dow"] / 7.0)
        df["dow_cos"] = np.cos(2 * np.pi * df["dow"] / 7.0)

        # month & season（1:春=3-5, 2:夏=6-8, 3:秋=9-11, 4:冬=12-2）
        df["month"] = df["date"].dt.month
        df["season"] = df["date"].apply(self._assign_season).astype(int)

        # holiday flags（jpholiday）
        hol = self._is_holiday_series(df["date"])
        df = df.join(hol)
        df["is_after_holiday"] = df["is_holiday"].shift(1, fill_value=0).astype(int)
        df["is_after_after_holiday"] = (
            df["is_holiday"].shift(2, fill_value=0).astype(int)
        )

        # event flags
        df["is_new_year"] = df["date"].apply(self._is_new_year).astype(int)
        df["is_obon"] = df["date"].apply(self._is_obon).astype(int)

        # lags & rolling
        df["lag_1"] = df["actual_volume"].shift(1)
        df["lag_7"] = df["actual_volume"].shift(7)
        df["rolling_mean_7"] = df["actual_volume"].shift(1).rolling(7).mean()

        # price flag は _load_mail で 0/1 化済み
        for c in FEATURE_COLUMNS:
            if c not in df.columns:
                df[c] = 0

        return df

    def _features_df(self, *, dropna: bool) -> pd.DataFrame:
        base = self._load_mail()
        out = self._add_features(base)
        if dropna:
            # 学習では特徴量の欠損行を落とし、目的変数も欠損なしに限定
            out = out.dropna(subset=FEATURE_COLUMNS + ["actual_volume"])
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
        dropna=False にすると将来日の行も残す（actual_volume が NULL のまま）。
        """
        df = self._features_df(dropna=dropna)
        cols = ["date", "office_id", "actual_volume"] + FEATURE_COLUMNS
        return df[cols]

    def predict(
        self,
        target_date: str | date,
        *,
        run_id: Optional[str] = None,
        stage: Optional[str] = "Production",
        model_name: str = "posms",
        tracking_uri: Optional[str] = None,
    ) -> float:
        """
        単一日の予測値を返す（office_id 固定）。

        既存 DB に対象日の行が存在しなくても、履歴から 1 行合成して特徴量を作る
        （lag/rolling は履歴ベース）。直近の実績が不足している場合はエラー。
        """
        base = self._load_mail()
        tgt = pd.to_datetime(str(target_date)).date()

        # 対象日行が無ければ 1 行合成（actual_volume=NULL, price_increase_flag=0）
        if (base["date"].dt.date == tgt).sum() == 0:
            new_row = {
                "date": pd.Timestamp(tgt),
                "office_id": self.office_id,
                "actual_volume": np.nan,
                "price_increase_flag": 0,
            }
            base = pd.concat(
                [base, pd.DataFrame([new_row])],
                ignore_index=True,
            ).sort_values("date")

        # 特徴量作成（dropna しない）
        all_df = self._add_features(base).reset_index(drop=True)
        row = all_df.loc[all_df["date"].dt.date == tgt]
        if row.empty:
            raise ValueError(
                f"対象日の行が作成できませんでした: date={tgt}, office_id={self.office_id}"
            )

        # 必要特徴量が欠損なら学習時と同等の入力がつくれない
        if row[FEATURE_COLUMNS].isna().any(axis=None):
            raise ValueError(
                "対象日の特徴量に欠損があります。直近の実績（前日および過去7日）と祝日情報を投入してください。"
            )

        X_row = row[FEATURE_COLUMNS].astype(float)
        pred = ModelPredictor(
            run_id=run_id,
            stage=stage,
            model_name=model_name,
            tracking_uri=tracking_uri,
        ).predict(X_row)[0]
        LOGGER.info(
            "Predict %s → %.2f (office_id=%s, mail_kind=%s)",
            tgt,
            pred,
            self.office_id,
            self.mail_kind,
        )
        return float(pred)
