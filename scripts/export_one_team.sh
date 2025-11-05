#!/usr/bin/env bash
set -euo pipefail

PY="/Users/hiraokatatsuru/miniconda3/envs/posms311/bin/python"
DC="DPT-A"
TEAM="${1:-1班}"
TEMPLATE="excel_templates/shift_template.xlsm"
OUT_DIR="excel_out"

mkdir -p "$OUT_DIR"

OUT="${OUT_DIR}/${TEAM}データ.xlsx"

echo "▶ 班 ${TEAM} 出力中..."

"$PY" -m posms export-team-workbook \
  --department-code "$DC" \
  --team "$TEAM" \
  --out "$OUT" \
  --template "$TEMPLATE"

echo "✅ ${TEAM} のファイル出力完了 → ${OUT}"

