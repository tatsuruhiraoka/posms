# syntax=docker/dockerfile:1.6

# ---------- Build stage ----------
FROM python:3.11-slim AS builder

ENV POETRY_VERSION=1.8.3 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_PREFER_BINARY=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ビルドに必要な OS パッケージ
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Poetry install
RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

# 依存解決（poetry.lock がない場合は失敗させる）
COPY pyproject.toml poetry.lock* /app/
RUN test -f poetry.lock || (echo "ERROR: poetry.lock がありません" && exit 1)

# キャッシュを効かせて高速化
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=cache,target=/root/.cache/pypoetry \
    poetry install --no-root --only main

# ---------- Runtime stage ----------
FROM python:3.11-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_PREFER_BINARY=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ランタイムに必要な共有ライブラリ
RUN apt-get update && \
    apt-get install -y --no-install-recommends libgomp1 libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Python deps をコピー
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# アプリソース
COPY . /app

# Prefect のURLは docker-compose.yml で上書き推奨
CMD ["python", "posms/flows/monthly_flow.py"]
