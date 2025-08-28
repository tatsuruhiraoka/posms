"""
posms.features.builder
======================

FeatureBuilder
--------------

- DB から mail_data を読み込み
- 日付系 / ラグ / 移動平均 などの特徴量を生成
- X (pandas.DataFrame) と y (Series) を返す

推論時は最新モデル (run_id 指定可) を MLflow からロードし、
単一日の需要予測 `predict()` を提供する。
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Tuple

import mlflow
import pandas as pd
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from xgboost import XGBRegressor

LOGGER = logging.getLogger("posms.features.builder")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class FeatureBuilder:
    """特徴量 DataFrame を構築するヘルパー"""

    def __init__(self, base_dir: Path | None = None) -> None:
        base_dir = base_dir or Path(__file__).resolve().parents[2]
        env_path = base_dir / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        db_url = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
            f"/{os.getenv('POSTGRES_DB')}"
        )
        self.engine = create_engine(db_url)

    # ------------------------------------------------------------------
    # 1. データロード
    # ------------------------------------------------------------------
    def _load_mail(self) -> pd.DataFrame:
        query = "SELECT mail_date, mail_count, is_holiday, price_increase_flag FROM mail_data"
        df = pd.read_sql(query, self.engine, parse_dates=["mail_date"])
        df = df.sort_values("mail_date")
        return df

    # ------------------------------------------------------------------
    # 2. 特徴量生成
    # ------------------------------------------------------------------
    def _add_date_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df["weekday"] = df["mail_date"].dt.weekday
        df["month"] = df["mail_date"].dt.month
        df["day"] = df["mail_date"].dt.day
        return df

    def _add_lag_features(self, df: pd.DataFrame, lags: Tuple[int, ...] = (1, 7)) -> pd.DataFrame:
        for lag in lags:
            df[f"lag_{lag}"] = df["mail_count"].shift(lag)
        return df

    def _add_rolling_mean(self, df: pd.DataFrame, window: int = 7) -> pd.DataFrame:
        df[f"roll_mean_{window}"] = df["mail_count"].shift(1).rolling(window).mean()
        return df

    # ------------------------------------------------------------------
    # 3. 外部 API
    # ------------------------------------------------------------------
    def build(self) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Returns
        -------
        X : pandas.DataFrame
        y : pandas.Series
        """
        df = self._load_mail()
        df = (
            df.pipe(self._add_date_features)
            .pipe(self._add_lag_features)
            .pipe(self._add_rolling_mean)
            .dropna()
        )

        y = df["mail_count"]
        X = df.drop(columns=["mail_count", "mail_date"])
        LOGGER.info("Feature matrix built: %s rows, %s columns", *X.shape)
        return X, y

    def predict(self, predict_date: str | date, run_id: str | None = None) -> int:
        """単一日 `predict_date` の需要を予測し整数で返す"""
        if isinstance(predict_date, str):
            predict_date = date.fromisoformat(predict_date)

        # build features for the target day based on historical df
        df_hist = self._load_mail()
        last_row = df_hist.iloc[-1:].copy()

        # extend to target date (simple example: previous day)
        target_row = last_row.copy()
        target_row["mail_date"] = predict_date
        target_row["mail_count"] = pd.NA
        df = pd.concat([df_hist, target_row], ignore_index=True)

        df = (
            df.pipe(self._add_date_features)
            .pipe(self._add_lag_features)
            .pipe(self._add_rolling_mean)
        ).tail(1)

        X_pred = df.drop(columns=["mail_count", "mail_date"])

        # モデルロード
        if run_id:
            model_uri = f"runs:/{run_id}/model"
        else:
            model_uri = "models:/posms/Production"

        model: XGBRegressor = mlflow.pyfunc.load_model(model_uri).unwrap_python_model()  # type: ignore
        pred = int(model.predict(X_pred)[0])
        LOGGER.info("Predict %s → %d", predict_date, pred)
        return pred

    # ------------------------------------------------------------------
    # 4. ユーティリティ
    # ------------------------------------------------------------------
    def load_staff(self) -> pd.DataFrame:
        """employees テーブルを DataFrame で取得"""
        return pd.read_sql("SELECT * FROM employees", self.engine)

    @staticmethod
    def load_yaml(path: Path) -> dict:
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh)


# ---------------- CLI テスト ----------------
if __name__ == "__main__":
    fb = FeatureBuilder()
    X, y = fb.build()
    print(X.head())
    print("Rows:", len(X), "Target mean:", y.mean())
