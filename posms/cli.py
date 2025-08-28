"""
posms.cli
=========

Command‑line interface for the Postal Operation Shift‑Management System.

Usage examples
--------------

# 予測 + シフト最適化 (E2E)
poetry run posms run-monthly --predict-date 2025-08-01

# 予測モデルだけ再学習
poetry run posms train

# シフトだけ再最適化（分担表案）
poetry run posms optimize --date 2025-08-01 --output-type 分担表案
"""

from __future__ import annotations

from pathlib import Path
from datetime import date
import typer
from typing_extensions import Annotated

from posms.flows.monthly_flow import monthly_refresh
from posms.models.trainer import ModelTrainer
from posms.features.builder import FeatureBuilder
from posms.optimization.shift_builder import ShiftBuilder, OutputType

app = typer.Typer(help="Postal Operation Shift‑Management System CLI")


# ---------- Helper -------------------------------------------------
def _default_template() -> Path:
    return Path("excel_templates/shift_template.xlsx")


# ---------- Commands ----------------------------------------------
@app.command("run-monthly")
def run_monthly(
    predict_date: Annotated[
        str,
        typer.Option(
            "--predict-date",
            "-d",
            help="YYYY-MM-DD 形式。省略時は翌月 1 日。",
        ),
    ] = "",
    output_type: Annotated[
        OutputType,
        typer.Option(
            "--output-type",
            "-o",
            case_sensitive=False,
            help="Excel 出力の種類: 分担表 / 勤務指定表 / 分担表案",
        ),
    ] = OutputType.分担表,
    excel_template: Annotated[
        Path,
        typer.Option("--template", "-t", help="Excel テンプレート .xlsx"),
    ] = _default_template(),
):
    """
    1. Excel → DB 取込
    2. XGBoost 再学習 (MLflow ログ)
    3. 需要予測 → PuLP シフト最適化
    4. Excel に書き出し
    """
    params = {
        "predict_date": predict_date or str(date.today()),
        "output_type": output_type.value,
        "excel_template": str(excel_template),
    }
    monthly_refresh(**params)  # Prefect Flow をローカル関数として実行


@app.command("train")
def train_model(
    params_file: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help="YAML でハイパーパラメータ定義 (configs/model_params.yaml)",
        ),
    ] = Path("configs/model_params.yaml"),
):
    """モデル再学習のみを実行"""
    cfg = FeatureBuilder().load_yaml(params_file)
    X, y = FeatureBuilder().build()
    run_id = ModelTrainer(cfg).train(X, y)
    typer.echo(f"MLflow run_id: {run_id}")


@app.command("optimize")
def optimize_shift(
    date_str: Annotated[str, typer.Option("--date", "-d")] = str(date.today()),
    output_type: Annotated[
        OutputType,
        typer.Option(
            help="分担表 / 勤務指定表 / 分担表案",
            case_sensitive=False,
        ),
    ] = OutputType.分担表,
    template: Annotated[
        Path,
        typer.Option("--template", "-t", help="Excel テンプレート"),
    ] = _default_template(),
):
    """需要予測済みデータを入力にシフトのみ再最適化"""
    demand = FeatureBuilder().predict(date_str)  # 既定 run_id を内部でロード
    staff = FeatureBuilder().load_staff()
    out = ShiftBuilder(template).build(demand, staff, output_type)
    typer.echo(f"Excel saved → {out.resolve()}")


if __name__ == "__main__":
    app()
