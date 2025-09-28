FROM python:3.11-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_PREFER_BINARY=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ランタイムに必要な OS ライブラリ（ホイール前提で最小限）
# - libgomp1: scikit-learn / xgboost の OpenMP
# - libpq5:   psycopg2-binary のランタイム
# - git:      dvc が内部で使用
# - tzdata:   タイムゾーン
RUN apt-get update && \
    apt-get install -y --no-install-recommends libgomp1 libpq5 git tzdata curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 依存レイヤーをキャッシュしやすい順にメタデータ→コードの順でコピー
COPY pyproject.toml README.md LICENSE /app/
    

# アプリ本体
COPY posms /app/posms
# 開発中は -e . にするとソース変更が即反映（本番は . の通常インストールでOK）
RUN pip install --no-cache-dir -e .
    

# 既定の実行（必要に応じて変更）
CMD ["posms", "--help"]
