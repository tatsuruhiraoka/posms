import numpy as np
import pandas as pd
from posms.models import ModelTrainer
from mlflow.tracking import MlflowClient


def _dummy_dataset(n: int = 50):
    rng = np.random.default_rng(42)
    X = pd.DataFrame(
        {
            "weekday": rng.integers(0, 6, n),
            "month": rng.integers(1, 12, n),
            "lag_1": rng.integers(9000, 13000, n),
        }
    )
    y = X["lag_1"] * 1.1 + rng.normal(0, 500, n)
    return X, y


def test_model_trainer_logs_to_mlflow(tmp_mlflow_env):
    X, y = _dummy_dataset(30)

    trainer = ModelTrainer(
        params={"n_estimators": 10, "max_depth": 3},
        experiment="unit_test",
    )
    run_id = trainer.train(X, y)

    # run_id が返るか
    assert isinstance(run_id, str) and run_id, "run_id が空です"

    # MLflow にメトリクスが残っているか
    client = MlflowClient()
    data = client.get_run(run_id).data
    assert "rmse" in data.metrics, "rmse が MLflow に記録されていません"
