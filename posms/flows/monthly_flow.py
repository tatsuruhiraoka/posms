"""
posms.flows.monthly_flow
========================

役割
----
- 毎月1日にモデル再学習を実行する Prefect Flow。

前提
----
- データは既に DB に投入済み（Excel は使わない）。
- 特徴量生成は FeatureBuilder().build() に委譲。
- 学習・MLflow へのロギングは ModelTrainer に委譲。

使い方
------
# 即時実行（テスト）
>>> from posms.flows.monthly_flow import monthly_train
>>> monthly_train(force=True)

# Prefect デプロイ（UIで毎月1日のスケジュールを付与）
$ prefect deployment build posms/flows/monthly_flow.py:monthly_train -n eom-train -p posms-pool -a
"""

from __future__ import annotations

import os
import logging
from datetime import date
from typing import Optional, Tuple

import pandas as pd
from prefect import flow, task, get_run_logger

from posms.features.builder import FeatureBuilder
from posms.models.trainer import ModelTrainer

LOGGER = logging.getLogger("posms.flows.monthly_flow")


# ---------------- 内部ユーティリティ ----------------
def _is_first_day_of_month(d: date) -> bool:
    """本日が「毎月1日」かどうか。"""
    return d.day == 1


# ---------------- Tasks ----------------
@task(name="Build features from DB")
def build_features_from_db() -> Tuple[pd.DataFrame, pd.Series]:
    """
    DB から学習用の (X, y) を構築。
    実装はプロジェクトの FeatureBuilder に依存。
    """
    X, y = FeatureBuilder().build()  # -> (pd.DataFrame, pd.Series)
    if len(X) == 0 or len(y) == 0:
        raise RuntimeError("学習用特徴量が空です（DB のデータ期間を確認してください）。")
    return X, y


@task(name="Train model")
def train_model(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    experiment: str,
    tracking_uri: str,
    auto_register: bool,
    val_split: float,
    es_rounds: int,
) -> str:
    """
    ModelTrainer で学習し、MLflow にロギング。
    auto_register=True の場合、MLflow Model Registry に登録まで行う
    （※Registry を使うには MLflow バックエンドを SQLite/Postgres に）。
    """
    trainer = ModelTrainer(experiment=experiment, tracking_uri=tracking_uri)
    run_id = trainer.train(
        X=X,
        y=y,
        auto_register=auto_register,
        val_split=val_split,
        es_rounds=es_rounds,
        tags={"pipeline": "monthly_train"},
    )
    return run_id


# ---------------- Flow ----------------
@flow(name="monthly_train")
def monthly_train(
    *,
    # 実行ガード
    first_day_guard: bool = True,      # True: 「毎月1日」以外はスキップ
    force: bool = False,               # テスト用: True でガード無視
    # 学習ハイパー
    val_split: float = 0.2,
    es_rounds: int = 50,
    # MLflow/Registry
    mlflow_experiment: Optional[str] = None,   # 既定: env MLFLOW_EXPERIMENT_NAME or "posms"
    mlflow_tracking_uri: Optional[str] = None, # 既定: env MLFLOW_TRACKING_URI or "http://mlflow:5000"
    auto_register: bool = False,               # True にするなら MLflow を SQL バックエンドに
) -> dict:
    """
    毎月1日にモデル再学習だけを行うフロー（Excel 不使用、DB のみ）。

    Returns
    -------
    dict: {"run_id": str|None, "skipped": bool, "today": "YYYY-MM-DD", "n_samples": int}
    """
    logger = get_run_logger()

    # --- 1日ガード ---
    today = date.today()
    if first_day_guard and (not force) and (not _is_first_day_of_month(today)):
        msg = f"Skip: {today.isoformat()} は毎月1日ではありません（force=False）"
        logger.info(msg)
        return {"skipped": True, "today": today.isoformat(), "run_id": None, "n_samples": 0}

    # --- 特徴量構築 ---
    X, y = build_features_from_db()
    n_samples = len(X)

    # --- MLflow 設定 ---
    experiment = mlflow_experiment or os.getenv("MLFLOW_EXPERIMENT_NAME", "posms")
    tracking_uri = mlflow_tracking_uri or os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    # --- 学習 ---
    run_id = train_model(
        X,
        y,
        experiment=experiment,
        tracking_uri=tracking_uri,
        auto_register=auto_register,
        val_split=val_split,
        es_rounds=es_rounds,
    )

    logger.info("Monthly training completed. run_id=%s, samples=%s", run_id, n_samples)
    return {"skipped": False, "today": today.isoformat(), "run_id": run_id, "n_samples": n_samples}


# ---------------- CLI / Debug -----------------
if __name__ == "__main__":
    # デバッグ実行（強制実行）
    print(monthly_train(force=True))
