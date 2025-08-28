from tempfile import TemporaryDirectory

import mlflow
import pytest


@pytest.fixture(autouse=True)
def tmp_mlflow_env(monkeypatch: pytest.MonkeyPatch):
    """MLflow の tracking URI を一時ディレクトリへ"""
    with TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("MLFLOW_TRACKING_URI", f"file://{tmpdir}")
        mlflow.set_tracking_uri(f"file://{tmpdir}")
        yield
