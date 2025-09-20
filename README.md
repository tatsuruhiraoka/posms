# postal-operation-shift-management-system

郵便物数の予測とシフト最適化を組み合わせた最小構成のデモプロジェクトです。

## ゼロ設定 MLflow

学習と推論は追加設定なしで動作します。トラッキングサーバを設定していない場合、実行結果はローカルの `mlruns/` ディレクトリに保存されます。外部の MLflow サーバを使う場合は、ライブラリを呼び出す前に環境変数 `MLFLOW_TRACKING_URI` を設定してください。Model Registry が利用できない（例: ローカルの file ストア）環境では、Predictor は自動的に対象 Experiment 内の最新 run にフォールバックします。

## クイックスタート（ゼロ設定）

```bash
# 1) インストール例
pip install -e .          # もしくは: poetry install

# 2) 最小の学習＆推論
python - <<'PY'
from posms.models import ModelTrainer, ModelPredictor
import pandas as pd, numpy as np
X = pd.DataFrame({"x": np.arange(50)})
y = X["x"] * 2 + 1 + np.random.randn(50)*0.1
run_id = ModelTrainer().train(X, y)          # ./mlruns に保存
pred   = ModelPredictor().predict(X.head())  # Registry 不可 → 最新 run にフォールバック
print("run_id:", run_id, "pred_shape:", pred.shape)
PY

# (任意) ローカルで実行履歴を閲覧
mlflow ui --backend-store-uri mlruns
