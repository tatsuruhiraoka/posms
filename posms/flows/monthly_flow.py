from __future__ import annotations

import os
import logging
from datetime import date
from typing import Optional, Tuple

import pandas as pd
import json
from pathlib import Path
from mlflow.tracking import MlflowClient
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

from posms.features.builder import FeatureBuilder

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
def build_features_from_db(*, office_id: int, mail_kind: str) -> Tuple[pd.DataFrame, pd.Series]:
    X, y = FeatureBuilder(office_id=office_id, mail_kind=mail_kind).build()
    if len(X) == 0 or len(y) == 0:
        raise RuntimeError("学習用特徴量が空です（DB のデータ期間を確認してください）。")
    return X, y


@task(
    name="Train model",
    retries=2,
    retry_delay_seconds=120,
    timeout_seconds=3600,
    log_prints=True,
)
def train_model(wsl -l -v

    X: pd.DataFrame,
    y: pd.Series,
    *,
    mail_kind: str,
    experiment: str,
    tracking_uri: str,
    auto_register: bool,
    val_split: float,
    es_rounds: int,
    tags: dict,
) -> str:
    # ★ mail_kind ごとに Trainer を切り替える
    TRAINER_PATH = {
        # ここはあなたの実フォルダ名に合わせる
        "normal": "posms.models.normal.trainer",
        "registered_plus": "posms.models.registered_plus.trainer",
    }

    if mail_kind not in TRAINER_PATH:
        raise ValueError(f"Unknown mail_kind for trainer: {mail_kind}")

    import importlib
    mod = importlib.import_module(TRAINER_PATH[mail_kind])
    ModelTrainer = getattr(mod, "ModelTrainer")

    trainer = ModelTrainer(experiment=experiment, tracking_uri=tracking_uri)
    run_id = trainer.train(
        X=X,
        y=y,
        auto_register=auto_register,
        val_split=val_split,
        es_rounds=es_rounds,
        tags=tags,
    )
    return run_id

@task(
    name="Export model bundle",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=300,
    log_prints=True,
)
def export_model_bundle(
    *,
    run_id: str,
    tracking_uri: str,
    experiment: str,
    mail_kind: str,
    feature_columns: list[str],
    out_dir: str = "model_bundle",
) -> dict:
    """
    MLflow run の artifacts から model.xgb をダウンロードし、配布用 bundle を作る。
    出力:
      model_bundle/<mail_kind>/
        model.xgb
        features.json
        meta.json
    """
    logger = get_run_logger()
    client = MlflowClient(tracking_uri=tracking_uri)

    # どこに書くか：プロジェクト直下を基準（実行場所が変わっても安定）
    project_root = Path(__file__).resolve().parents[2]
    dst = project_root / out_dir / mail_kind
    dst.mkdir(parents=True, exist_ok=True)

    # model.xgb を artifacts から取得（mlflow.xgboost フレーバーの標準）
    # trainer.train の artifact_path="model" 前提で "model/model.xgb"
    local_model_path = client.download_artifacts(run_id, "model/model.xgb", dst.as_posix())
    # download_artifacts は dst 配下に model/model.xgb を作るので、分かりやすく直下へコピー/移動
    src = Path(local_model_path)
    (dst / "model.xgb").write_bytes(src.read_bytes())

    # features.json
    (dst / "features.json").write_text(
        json.dumps(feature_columns, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # meta.json（最低限）
    meta = {
        "experiment": experiment,
        "mail_kind": mail_kind,
        "run_id": run_id,
    }
    (dst / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info("Exported model bundle: %s", dst)
    return {"mail_kind": mail_kind, "bundle_dir": str(dst)}

# ---------------- Flow ----------------
@flow(
    name="monthly_train",
    log_prints=True,
    flow_run_name="monthly-train-{run_date}-{force}",
)
def monthly_train(
    *,
    run_date: Optional[date] = None,
    first_day_guard: bool = True,
    force: bool = False,
    # ★追加：どの局/どのモデルを回すか
    office_id: int = 1,
    mail_kinds: Tuple[str, ...] = ("normal", "registered_plus"),
    # 学習ハイパー
    val_split: float = 0.2,
    es_rounds: int = 50,
    # MLflow/Registry
    mlflow_experiment: Optional[str] = None,
    mlflow_tracking_uri: Optional[str] = None,
    auto_register: bool = False,
    # ★追加：配布用bundle出力先
    model_bundle_dir: str = "model_bundle",
) -> dict:
    logger = get_run_logger()
    run_date = run_date or date.today()

    if first_day_guard and (not force) and (not _is_first_day_of_month(run_date)):
        msg = f"Skip: {run_date.isoformat()} は毎月1日ではありません（force=False）"
        logger.info(msg)
        create_markdown_artifact(key="monthly-train-summary", markdown=f"**Skipped**: {msg}")
        return {"skipped": True, "today": run_date.isoformat(), "results": {}}

    experiment = mlflow_experiment or os.getenv("MLFLOW_EXPERIMENT_NAME", "posms")
    tracking_uri = mlflow_tracking_uri or os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    results = {}

    for mail_kind in mail_kinds:
        logger.info("=== monthly_train start: mail_kind=%s ===", mail_kind)

        # 特徴量
        X, y = build_features_from_db(office_id=office_id, mail_kind=mail_kind)
        n_samples = len(X)
        feature_cols = list(X.columns)

        # ★迷子根絶タグ（超重要）
        tags = {
            "pipeline": "monthly_train",
            "mail_kind": mail_kind,
            "feature_columns": ",".join(feature_cols),
            "office_id": str(office_id),
        }

        # 学習
        run_id = train_model(
            X, y,
            experiment=experiment,
            tracking_uri=tracking_uri,
            auto_register=auto_register,
            val_split=val_split,
            es_rounds=es_rounds,
            tags=tags,
        )

        # 配布用bundleへ書き出し
        bundle_info = export_model_bundle(
            run_id=run_id,
            tracking_uri=tracking_uri,
            experiment=experiment,
            mail_kind=mail_kind,
            feature_columns=feature_cols,
            out_dir=model_bundle_dir,
        )

        # Prefect artifact（kindごと）
        create_markdown_artifact(
            key=f"monthly-train-{mail_kind}",
            markdown=(
                f"**Monthly training completed ({mail_kind})**  \n"
                f"- date: `{run_date.isoformat()}`  \n"
                f"- office_id: `{office_id}`  \n"
                f"- samples: `{n_samples}`  \n"
                f"- mlflow.run_id: `{run_id}`  \n"
                f"- experiment: `{experiment}`  \n"
                f"- tracking_uri: `{tracking_uri}`  \n"
                f"- bundle: `{bundle_info.get('bundle_dir')}`"
            ),
        )

        results[mail_kind] = {
            "run_id": run_id,
            "n_samples": n_samples,
            "bundle_dir": bundle_info.get("bundle_dir"),
        }

        logger.info("=== monthly_train done: mail_kind=%s run_id=%s ===", mail_kind, run_id)

    # 全体サマリ
    create_markdown_artifact(
        key="monthly-train-summary",
        markdown="## Monthly train summary\n" + "\n".join(
            [f"- `{k}`: run_id=`{v['run_id']}` bundle=`{v['bundle_dir']}`" for k, v in results.items()]
        ),
    )

    return {"skipped": False, "today": run_date.isoformat(), "results": results}


if __name__ == "__main__":
    print(monthly_train(force=True, run_date=date.today()))
