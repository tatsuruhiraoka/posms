# posms/_mlflow.py
"""
ゼロ設定（.env 不要）で MLflow の Tracking URI を初期化するヘルパー。

優先順位:
1) 関数引数 `tracking_uri` が指定されていればそれを採用
2) 環境変数 `MLFLOW_TRACKING_URI` があればそれを採用
3) どちらも無ければ、リポジトリ直下に `mlruns/` を作成し、その `file://.../mlruns`
   を Tracking URI として設定（クロスプラットフォームで安定するよう URI 化）
"""

from __future__ import annotations

from pathlib import Path
import os
import mlflow

__all__ = ["set_tracking_uri_zero_config", "_default_tracking_dir"]


def _default_tracking_dir() -> Path:
    """
    既定の MLflow 保存先ディレクトリ（<repo>/mlruns）を返す。
    ディレクトリが無ければ作成する。
    """
    # このファイルは <repo>/posms/_mlflow.py にある想定。
    # 親（posms）のさらに親がリポジトリルート。
    repo_root = Path(__file__).resolve().parents[1]
    path = repo_root / "mlruns"
    path.mkdir(parents=True, exist_ok=True)
    return path


def set_tracking_uri_zero_config(tracking_uri: str | None = None) -> None:
    """
    MLflow の Tracking URI を “ゼロ設定”方針で決定し、mlflow に反映する。

    Parameters
    ----------
    tracking_uri : str | None
        明示的に Tracking URI を指定したい場合に与える。
        省略時は環境変数 MLFLOW_TRACKING_URI、どちらも無ければ <repo>/mlruns を使用。
    """
    uri = tracking_uri or os.getenv("MLFLOW_TRACKING_URI")

    if not uri:
        # 既定は <repo>/mlruns を file:// スキームの URI として設定（表記揺れ防止）
        uri = _default_tracking_dir().resolve().as_uri()

    mlflow.set_tracking_uri(uri)
