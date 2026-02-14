from pathlib import Path
import shutil
from urllib.parse import urlparse, unquote

import numpy as np
import pandas as pd
import mlflow
import pytest
from mlflow.tracking import MlflowClient

from posms.models.normal.trainer import ModelTrainer
from posms.models.predictor import ModelPredictor


def _dummy_dataset(n: int = 50):
    rng = np.random.default_rng(42)
    X = pd.DataFrame(
        {
            "weekday": rng.integers(0, 7, n),  # 0–6
            "month": rng.integers(1, 13, n),  # 1–12
            "lag_1": rng.integers(9000, 13000, n),
        }
    )
    y = X["lag_1"] * 1.1 + rng.normal(0, 500, n)
    return X, y


@pytest.fixture
def tmp_mlflow_store(tmp_path, monkeypatch):
    """
    MLflow のトラッキングURIを一時ディレクトリに切り替える。
    ※ autouse=False（明示指定したテストでのみ有効）
    """
    uri = (tmp_path / "mlruns").as_uri()
    monkeypatch.setenv("MLFLOW_TRACKING_URI", uri)
    mlflow.set_tracking_uri(uri)
    return uri


def test_zero_config_trainer_and_predictor(monkeypatch):
    """
    ゼロ設定（環境変数なし）で <repo>/mlruns が生成され、
    Predictor が Registry 非依存で最新runから推論できること。
    """
    # 環境変数オフ & 既存mlruns掃除
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    if Path("mlruns").exists():
        shutil.rmtree("mlruns")

    # 軽量学習
    X, y = _dummy_dataset(30)
    run_id = ModelTrainer(params={"n_estimators": 10, "max_depth": 3}).train(X, y)
    assert isinstance(run_id, str) and run_id

    # 実際の保存先をURIから検証（将来パスが変わっても壊れない）
    uri = mlflow.get_tracking_uri()
    parsed = urlparse(uri)
    store_path = Path(unquote(parsed.path))
    assert store_path.exists(), f"tracking store not found: {uri}"

    # 推論（Registryなくても最新runへフォールバック）
    preds = ModelPredictor().predict(X.head())
    assert preds.shape[0] == len(X.head())


def test_trainer_logs_metrics_to_mlflow(tmp_mlflow_store):
    """
    一時ディレクトリのトラッキングストアへ学習結果が記録されること。
    """
    X, y = _dummy_dataset(30)
    trainer = ModelTrainer(
        params={"n_estimators": 10, "max_depth": 3}, experiment="unit_test"
    )
    run_id = trainer.train(X, y)
    assert isinstance(run_id, str) and run_id

    client = MlflowClient()
    data = client.get_run(run_id).data

    # どのTrainer実装でも拾えるようメトリクスキーを緩めにチェック
    # （シンプル版: "rmse"、完全版: "rmse_train"/"rmse_val"）
    metric_keys = set(data.metrics.keys())
    assert {"rmse", "mae"} <= metric_keys or {
        "rmse_train",
        "mae_train",
    } <= metric_keys, f"unexpected metrics: {metric_keys}"
