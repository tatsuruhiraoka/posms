"""
scripts/run_monthly.py
既存業務向けワンショットランチャー:
Prefect Flow ``monthly_refresh`` をローカル関数呼びで実行する。
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

# ★ 新しいパッケージ / フロー名に合わせる
from posms.flows.monthly_flow import monthly_refresh


# ---------------- Logger -----------------
def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger(__name__)


# ---------------- Config loader ----------
def load_flow_parameters(config_path: Path) -> dict:
    """
    YAML ファイルから Prefect Deployment の parameters セクションだけ抽出する。
    - configs/monthly_job.yaml は Prefect 2.14+ 推奨の `deployments:` 形式を想定
    """
    if not config_path.exists():
        return {}

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # deployments: - parameters: … という構造
    try:
        first_dep = cfg["deployments"][0]
        return first_dep.get("parameters", {}) or {}
    except (KeyError, IndexError, TypeError):
        # 古い形式 flow:deployment:parameters: … にも一応対応
        return cfg.get("flow", {}).get("deployment", {}).get("parameters", {}) or {}


# ---------------- Main -------------------
def main() -> None:
    logger = setup_logger()

    # config パスを解決
    config_path = (
        Path(__file__).resolve().parent.parent / "configs" / "monthly_job.yaml"
    )
    logger.info("Loading flow parameters from %s", config_path)
    params: dict = load_flow_parameters(config_path)
    logger.info("Parameters: %s", params or "(none)")

    # Prefect Flow を **直接 Python 関数として** 実行
    logger.info("Running Prefect flow locally …")
    monthly_refresh(**params)
    logger.info("Flow completed")


if __name__ == "__main__":
    main()
