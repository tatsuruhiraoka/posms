"""
posms.flows.monthly_flow
========================

月次バッチフロー : Excel → DB → XGBoost 再学習 → 需要予測 → シフト最適化

実行方法
--------
1. ローカル関数として
   >>> from posms.flows.monthly_flow import monthly_refresh
   >>> monthly_refresh(predict_date="2025-08-01")

2. CLI
   $ poetry run posms run-monthly --predict-date 2025-08-01

3. Prefect Deployment
   $ prefect deploy -n monthly configs/monthly_job.yaml
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from prefect import flow, task

from posms.etl.extractor import ExcelExtractor
from posms.etl.load_to_db import DbLoader
from posms.features.builder import FeatureBuilder
from posms.models.trainer import ModelTrainer
from posms.optimization.shift_builder import OutputType, ShiftBuilder

LOGGER = logging.getLogger("posms.flows.monthly_flow")


# ---------------- Prefect Tasks ----------------
@task(name="Extract Excel → CSV")
def extract_task() -> None:
    ExcelExtractor().run_all()


@task(name="Load CSV → PostgreSQL")
def load_task() -> None:
    DbLoader().run_all()


@task(name="Model Training")
def train_task() -> str:
    X, y = FeatureBuilder().build()
    run_id = ModelTrainer().train(X, y, auto_register=True)
    return run_id


@task(name="Demand Forecast")
def predict_task(predict_date: str, run_id: str) -> int:
    fb = FeatureBuilder()
    demand_val = fb.predict(predict_date, run_id)
    return demand_val


@task(name="Shift Optimization & Excel Output")
def optimize_task(
    predict_date: str,
    demand_val: int,
    output_type: OutputType,
    template: Path,
) -> Path:
    # demand Series を1日分で組み立て
    demand_series = (
        FeatureBuilder()
        ._load_mail()  # noqa: SLF001 使用済の内部メソッドだが簡易に
        .iloc[[-1]]
        .assign(mail_date=predict_date, mail_count=demand_val)
        .set_index("mail_date")["mail_count"]
    )

    staff_df = FeatureBuilder().load_staff()
    sb = ShiftBuilder(template)
    return sb.build(demand_series, staff_df, output_type)


# ---------------- Prefect Flow ----------------
@flow(name="monthly_refresh")
def monthly_refresh(
    predict_date: str = str(date.today()),
    output_type: str = "分担表",
    excel_template: str = "excel_templates/shift_template.xlsx",
):
    """
    Parameters
    ----------
    predict_date : str
        需要予測 & シフト対象日 (YYYY-MM-DD)
    output_type : str
        分担表 / 勤務指定表 / 分担表案
    excel_template : str
        テンプレート Excel パス
    """
    # ETL
    extract_task()
    load_task()

    # ML 学習
    run_id = train_task()

    # 需要予測
    demand_val = predict_task(predict_date, run_id)

    # シフト最適化
    excel_path = optimize_task(
        predict_date,
        demand_val,
        OutputType(output_type),
        Path(excel_template),
    )

    LOGGER.info("Flow succeeded. Excel saved → %s", excel_path.resolve())
    return {"excel_path": str(excel_path)}


# ---------------- CLI / Debug -----------------
if __name__ == "__main__":
    monthly_refresh()
