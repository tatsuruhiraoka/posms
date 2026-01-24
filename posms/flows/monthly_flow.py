from __future__ import annotations

import os
import logging
from datetime import date
from typing import Optional, Tuple

import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

from posms.features.builder import FeatureBuilder
from posms.models.trainer import ModelTrainer

LOGGER = logging.getLogger("posms.flows.monthly_flow")


# ---------------- 内部ユーティリティ ----------------
def _is_first_day_of_month(d: date) -> bool:
    return d.day == 1


# ---------------- Tasks ----------------
@task(
    name="Build features from DB",
    retries=3,
    retry_delay_seconds=60,
    timeout_seconds=300,  # 5分でタイムアウト
    log_prints=True,
)
def build_features_from_db() -> Tuple[pd.DataFrame, pd.Series]:
    X, y = FeatureBuilder().build()
    if len(X) == 0 or len(y) == 0:
        raise RuntimeError(
            "学習用特徴量が空です（DB のデータ期間を確認してください）。"
        )
    return X, y


@task(
    name="Train model",
    retries=2,
    retry_delay_seconds=120,
    timeout_seconds=3600,  # 最長1時間（必要に応じて調整）
    log_prints=True,
)
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
@flow(
    name="monthly_train",
    log_prints=True,
    flow_run_name="monthly-train-{run_date}-{force}",
)
def monthly_train(
    *,
    # 実行日（未指定なら今日）。バックフィルや検証に便利
    run_date: Optional[date] = None,
    # 実行ガード
    first_day_guard: bool = True,
    force: bool = False,
    # 学習ハイパー
    val_split: float = 0.2,
    es_rounds: int = 50,
    # MLflow/Registry
    mlflow_experiment: Optional[str] = None,
    mlflow_tracking_uri: Optional[str] = None,
    auto_register: bool = False,
) -> dict:
    """
    毎月1日にモデル再学習（DBのみ）。
    Returns: {"run_id": str|None, "skipped": bool, "today": "YYYY-MM-DD", "n_samples": int}
    """
    logger = get_run_logger()

    # --- 実行日決定 ---
    run_date = run_date or date.today()

    # --- 1日ガード ---
    if first_day_guard and (not force) and (not _is_first_day_of_month(run_date)):
        msg = f"Skip: {run_date.isoformat()} は毎月1日ではありません（force=False）"
        logger.info(msg)
        create_markdown_artifact(
            key="monthly-train-summary", markdown=f"**Skipped**: {msg}"
        )
        return {
            "skipped": True,
            "today": run_date.isoformat(),
            "run_id": None,
            "n_samples": 0,
        }

    # --- 特徴量構築 ---
    X, y = build_features_from_db()
    n_samples = len(X)

    # --- MLflow 設定 ---
    experiment = mlflow_experiment or os.getenv("MLFLOW_EXPERIMENT_NAME", "posms")
    tracking_uri = mlflow_tracking_uri or os.getenv(
        "MLFLOW_TRACKING_URI", "http://mlflow:5000"
    )

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

    # --- 要点をArtifactsに残す（UIのArtifactsタブで確認可能） ---
    create_markdown_artifact(
        key="monthly-train-summary",
        markdown=(
            f"**Monthly training completed**  \n"
            f"- date: `{run_date.isoformat()}`  \n"
            f"- samples: `{n_samples}`  \n"
            f"- mlflow.run_id: `{run_id}`  \n"
            f"- experiment: `{experiment}`  \n"
            f"- tracking_uri: `{tracking_uri}`"
        ),
    )

    logger.info("Monthly training completed. run_id=%s, samples=%s", run_id, n_samples)
    return {
        "skipped": False,
        "today": run_date.isoformat(),
        "run_id": run_id,
        "n_samples": n_samples,
    }


if __name__ == "__main__":
    print(monthly_train(force=True, run_date=date.today()))
