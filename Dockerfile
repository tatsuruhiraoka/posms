# ---------- Build stage ----------
FROM python:3.11-slim AS builder

ENV POETRY_VERSION=1.5.1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

# ビルドに必要な OS パッケージ
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Poetry install
RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

# 依存解決
COPY pyproject.toml poetry.lock* /app/
RUN poetry install --no-root --only main --no-interaction

# ---------- Runtime stage ----------
FROM python:3.11-slim

WORKDIR /app

# ランタイムに必要な共有ライブラリ
RUN apt-get update && \
    apt-get install -y --no-install-recommends libgomp1 libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Python deps をコピー
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/* /usr/local/bin/

# アプリソース
COPY . /app

# Prefect ローカル API (クラウドの場合は .env で上書き)
ENV PREFECT_API_URL=http://127.0.0.1:4200/api

CMD ["python", "posms/flows/monthly_flow.py"]
