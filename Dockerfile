# syntax=docker/dockerfile:1.6
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
    apt-get install -y --no-install-recommends libgomp1 libpq5 git tzdata && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 依存を先に入れてレイヤーキャッシュを効かせる
COPY requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r /app/requirements.txt

# アプリ本体
COPY . /app

# 既定の実行（必要に応じて変更）
CMD ["python", "posms/flows/monthly_flow.py"]
